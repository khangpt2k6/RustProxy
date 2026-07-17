use crate::config::Strategy;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct Backend {
    pub addr: String,
    healthy: AtomicBool,
    active_conns: AtomicUsize,
    consecutive_fails: AtomicU32,
    consecutive_oks: AtomicU32,
}

impl Backend {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            // start healthy so we serve traffic before the first probe lands
            healthy: AtomicBool::new(true),
            active_conns: AtomicUsize::new(0),
            consecutive_fails: AtomicU32::new(0),
            consecutive_oks: AtomicU32::new(0),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    pub fn active_conns(&self) -> usize {
        self.active_conns.load(Ordering::Relaxed)
    }

    pub fn inc_conns(&self) {
        self.active_conns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_conns(&self) {
        self.active_conns.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a probe result. fall/rise thresholds keep a single flaky probe
    /// from flapping the backend in and out of rotation.
    pub fn record_probe(&self, ok: bool, fall: u32, rise: u32) -> Option<bool> {
        if ok {
            self.consecutive_fails.store(0, Ordering::Relaxed);
            let oks = self.consecutive_oks.fetch_add(1, Ordering::Relaxed) + 1;
            if !self.is_healthy() && oks >= rise {
                self.healthy.store(true, Ordering::Relaxed);
                return Some(true);
            }
        } else {
            self.consecutive_oks.store(0, Ordering::Relaxed);
            let fails = self.consecutive_fails.fetch_add(1, Ordering::Relaxed) + 1;
            if self.is_healthy() && fails >= fall {
                self.healthy.store(false, Ordering::Relaxed);
                return Some(false);
            }
        }
        None
    }
}

/// RAII guard so active connection counts stay correct even if the
/// request future is dropped mid-flight.
pub struct ConnGuard {
    backend: Arc<Backend>,
}

impl ConnGuard {
    pub fn new(backend: Arc<Backend>) -> Self {
        backend.inc_conns();
        Self { backend }
    }

    pub fn backend(&self) -> &Arc<Backend> {
        &self.backend
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.backend.dec_conns();
    }
}

pub struct BackendPool {
    backends: RwLock<Vec<Arc<Backend>>>,
    strategy: Strategy,
    rr_cursor: AtomicUsize,
}

impl BackendPool {
    pub fn new(addrs: Vec<String>, strategy: Strategy) -> Self {
        let backends = addrs
            .into_iter()
            .map(|a| Arc::new(Backend::new(a)))
            .collect();
        Self {
            backends: RwLock::new(backends),
            strategy,
            rr_cursor: AtomicUsize::new(0),
        }
    }

    pub fn snapshot(&self) -> Vec<Arc<Backend>> {
        self.backends.read().unwrap().clone()
    }

    pub fn add(&self, addr: String) -> bool {
        let mut guard = self.backends.write().unwrap();
        if guard.iter().any(|b| b.addr == addr) {
            return false;
        }
        guard.push(Arc::new(Backend::new(addr)));
        true
    }

    pub fn remove(&self, addr: &str) -> bool {
        let mut guard = self.backends.write().unwrap();
        let before = guard.len();
        guard.retain(|b| b.addr != addr);
        guard.len() != before
    }

    /// Pick a healthy backend according to the configured strategy.
    pub fn pick(&self) -> Option<Arc<Backend>> {
        let backends = self.backends.read().unwrap();
        let healthy: Vec<&Arc<Backend>> = backends.iter().filter(|b| b.is_healthy()).collect();
        if healthy.is_empty() {
            return None;
        }
        let chosen = match self.strategy {
            Strategy::RoundRobin => {
                let i = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % healthy.len();
                healthy[i]
            }
            Strategy::LeastConnections => healthy
                .iter()
                .min_by_key(|b| b.active_conns())
                .expect("non-empty"),
        };
        Some(Arc::clone(chosen))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_cycles_healthy_backends() {
        let pool = BackendPool::new(
            vec!["a".into(), "b".into(), "c".into()],
            Strategy::RoundRobin,
        );
        let picks: Vec<String> = (0..6).map(|_| pool.pick().unwrap().addr.clone()).collect();
        assert_eq!(picks, vec!["a", "b", "c", "a", "b", "c"]);
    }

    #[test]
    fn skips_unhealthy() {
        let pool = BackendPool::new(vec!["a".into(), "b".into()], Strategy::RoundRobin);
        let b = pool.snapshot()[1].clone();
        // fall threshold 1 -> single failed probe marks it down
        b.record_probe(false, 1, 1);
        for _ in 0..4 {
            assert_eq!(pool.pick().unwrap().addr, "a");
        }
    }

    #[test]
    fn least_connections_prefers_idle() {
        let pool = BackendPool::new(vec!["a".into(), "b".into()], Strategy::LeastConnections);
        let a = pool.snapshot()[0].clone();
        let _g1 = ConnGuard::new(a.clone());
        let _g2 = ConnGuard::new(a);
        assert_eq!(pool.pick().unwrap().addr, "b");
    }

    #[test]
    fn conn_guard_decrements_on_drop() {
        let pool = BackendPool::new(vec!["a".into()], Strategy::LeastConnections);
        let a = pool.snapshot()[0].clone();
        {
            let _g = ConnGuard::new(a.clone());
            assert_eq!(a.active_conns(), 1);
        }
        assert_eq!(a.active_conns(), 0);
    }

    #[test]
    fn fall_rise_thresholds() {
        let b = Backend::new("x".into());
        assert!(b.is_healthy());
        assert_eq!(b.record_probe(false, 3, 2), None);
        assert_eq!(b.record_probe(false, 3, 2), None);
        assert_eq!(b.record_probe(false, 3, 2), Some(false));
        assert!(!b.is_healthy());
        assert_eq!(b.record_probe(true, 3, 2), None);
        assert_eq!(b.record_probe(true, 3, 2), Some(true));
        assert!(b.is_healthy());
    }

    #[test]
    fn no_healthy_backends_returns_none() {
        let pool = BackendPool::new(vec!["a".into()], Strategy::RoundRobin);
        pool.snapshot()[0].record_probe(false, 1, 1);
        assert!(pool.pick().is_none());
    }

    #[test]
    fn add_remove() {
        let pool = BackendPool::new(vec!["a".into()], Strategy::RoundRobin);
        assert!(pool.add("b".into()));
        assert!(!pool.add("b".into()));
        assert_eq!(pool.snapshot().len(), 2);
        assert!(pool.remove("a"));
        assert!(!pool.remove("a"));
        assert_eq!(pool.snapshot().len(), 1);
    }
}
