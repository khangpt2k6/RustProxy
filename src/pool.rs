use crate::config::Strategy;
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Backend {
    pub addr: String,
    weight: AtomicU32,
    healthy: AtomicBool,
    active_conns: AtomicUsize,
    consecutive_fails: AtomicU32,
    consecutive_oks: AtomicU32,
}

impl Backend {
    pub fn new(addr: String, weight: u32) -> Self {
        Self {
            addr,
            weight: AtomicU32::new(weight),
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

    pub fn weight(&self) -> u32 {
        self.weight.load(Ordering::Relaxed)
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

/// Immutable routing snapshot. The proxy hot path only ever reads one of these
/// through an atomic load - no locks - while the control plane swaps in a fresh
/// snapshot on any change.
#[derive(Debug)]
struct Ring {
    backends: Vec<Arc<Backend>>,
    /// Precomputed pick order: indices into `backends`, laid out so each backend
    /// appears in proportion to its weight (smooth weighted round robin).
    schedule: Vec<usize>,
}

pub struct BackendPool {
    ring: ArcSwap<Ring>,
    strategy: Strategy,
    cursor: AtomicUsize,
    /// serializes control-plane writers so concurrent add/remove/set_weight
    /// can't clobber each other's copy-on-write snapshot. Never touched on the
    /// request path.
    write_lock: Mutex<()>,
}

impl BackendPool {
    pub fn new(backends: Vec<(String, u32)>, strategy: Strategy) -> Self {
        let backends: Vec<Arc<Backend>> = backends
            .into_iter()
            .map(|(addr, w)| Arc::new(Backend::new(addr, w)))
            .collect();
        Self {
            ring: ArcSwap::from_pointee(Ring::build(backends)),
            strategy,
            cursor: AtomicUsize::new(0),
            write_lock: Mutex::new(()),
        }
    }

    /// Convenience for callers that don't care about weights (all get weight 1).
    #[cfg(test)]
    pub fn from_addrs(addrs: Vec<String>, strategy: Strategy) -> Self {
        Self::new(addrs.into_iter().map(|a| (a, 1)).collect(), strategy)
    }

    pub fn snapshot(&self) -> Vec<Arc<Backend>> {
        self.ring.load().backends.clone()
    }

    pub fn add(&self, addr: String, weight: u32) -> bool {
        let _w = self.write_lock.lock().unwrap();
        let mut backends = self.ring.load().backends.clone();
        if backends.iter().any(|b| b.addr == addr) {
            return false;
        }
        backends.push(Arc::new(Backend::new(addr, weight)));
        self.ring.store(Arc::new(Ring::build(backends)));
        true
    }

    pub fn remove(&self, addr: &str) -> bool {
        let _w = self.write_lock.lock().unwrap();
        let mut backends = self.ring.load().backends.clone();
        let before = backends.len();
        backends.retain(|b| b.addr != addr);
        if backends.len() == before {
            return false;
        }
        self.ring.store(Arc::new(Ring::build(backends)));
        true
    }

    /// Shift traffic by changing a backend's weight at runtime. Rebuilds the
    /// weighted schedule so the new ratio takes effect on the next pick.
    pub fn set_weight(&self, addr: &str, weight: u32) -> bool {
        let _w = self.write_lock.lock().unwrap();
        let backends = self.ring.load().backends.clone();
        let Some(b) = backends.iter().find(|b| b.addr == addr) else {
            return false;
        };
        b.weight.store(weight, Ordering::Relaxed);
        self.ring.store(Arc::new(Ring::build(backends)));
        true
    }

    /// Pick a healthy backend according to the configured strategy. Lock-free:
    /// a single atomic load of the current ring plus, for round robin, one
    /// atomic increment of the cursor.
    pub fn pick(&self) -> Option<Arc<Backend>> {
        let ring = self.ring.load();
        if ring.backends.is_empty() {
            return None;
        }
        match self.strategy {
            Strategy::RoundRobin => {
                let sched = &ring.schedule;
                if sched.is_empty() {
                    return None;
                }
                // walk the weighted schedule from the cursor, skipping any
                // backend that's currently down.
                let start = self.cursor.fetch_add(1, Ordering::Relaxed);
                for i in 0..sched.len() {
                    let idx = sched[start.wrapping_add(i) % sched.len()];
                    let b = &ring.backends[idx];
                    if b.is_healthy() {
                        return Some(Arc::clone(b));
                    }
                }
                None
            }
            Strategy::LeastConnections => ring
                .backends
                .iter()
                .filter(|b| b.is_healthy())
                .min_by_key(|b| b.active_conns())
                .map(Arc::clone),
        }
    }
}

impl Ring {
    fn build(backends: Vec<Arc<Backend>>) -> Self {
        let weights: Vec<u32> = backends.iter().map(|b| b.weight()).collect();
        let schedule = build_schedule(&weights);
        Self { backends, schedule }
    }
}

fn gcd(a: u32, b: u32) -> u32 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// Build a smooth weighted round-robin pick order (the algorithm nginx uses).
/// Backends appear spread out in proportion to their weight rather than in
/// contiguous runs, so a 5:1 split alternates instead of firing five in a row.
/// Weight 0 drains a backend (it never appears); all-zero falls back to plain
/// equal round robin so a misconfig can't wedge routing.
fn build_schedule(weights: &[u32]) -> Vec<usize> {
    let n = weights.len();
    if n == 0 {
        return Vec::new();
    }
    // reduce by the gcd of the positive weights so [200,100] doesn't build a
    // 300-entry table when [2,1] does the same job.
    let g = weights
        .iter()
        .copied()
        .filter(|w| *w > 0)
        .reduce(gcd)
        .unwrap_or(1)
        .max(1);
    let w: Vec<i64> = weights.iter().map(|x| (*x / g) as i64).collect();
    let total: i64 = w.iter().sum();
    if total <= 0 {
        return (0..n).collect();
    }
    let mut current = vec![0i64; n];
    let mut seq = Vec::with_capacity(total as usize);
    for _ in 0..total {
        let mut best = usize::MAX;
        for i in 0..n {
            if w[i] <= 0 {
                continue;
            }
            current[i] += w[i];
            if best == usize::MAX || current[i] > current[best] {
                best = i;
            }
        }
        current[best] -= total;
        seq.push(best);
    }
    seq
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_cycles_healthy_backends() {
        let pool = BackendPool::from_addrs(
            vec!["a".into(), "b".into(), "c".into()],
            Strategy::RoundRobin,
        );
        let picks: Vec<String> = (0..6).map(|_| pool.pick().unwrap().addr.clone()).collect();
        assert_eq!(picks, vec!["a", "b", "c", "a", "b", "c"]);
    }

    #[test]
    fn skips_unhealthy() {
        let pool = BackendPool::from_addrs(vec!["a".into(), "b".into()], Strategy::RoundRobin);
        let b = pool.snapshot()[1].clone();
        // fall threshold 1 -> single failed probe marks it down
        b.record_probe(false, 1, 1);
        for _ in 0..4 {
            assert_eq!(pool.pick().unwrap().addr, "a");
        }
    }

    #[test]
    fn least_connections_prefers_idle() {
        let pool = BackendPool::from_addrs(vec!["a".into(), "b".into()], Strategy::LeastConnections);
        let a = pool.snapshot()[0].clone();
        let _g1 = ConnGuard::new(a.clone());
        let _g2 = ConnGuard::new(a);
        assert_eq!(pool.pick().unwrap().addr, "b");
    }

    #[test]
    fn conn_guard_decrements_on_drop() {
        let pool = BackendPool::from_addrs(vec!["a".into()], Strategy::LeastConnections);
        let a = pool.snapshot()[0].clone();
        {
            let _g = ConnGuard::new(a.clone());
            assert_eq!(a.active_conns(), 1);
        }
        assert_eq!(a.active_conns(), 0);
    }

    #[test]
    fn fall_rise_thresholds() {
        let b = Backend::new("x".into(), 1);
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
        let pool = BackendPool::from_addrs(vec!["a".into()], Strategy::RoundRobin);
        pool.snapshot()[0].record_probe(false, 1, 1);
        assert!(pool.pick().is_none());
    }

    #[test]
    fn add_remove() {
        let pool = BackendPool::from_addrs(vec!["a".into()], Strategy::RoundRobin);
        assert!(pool.add("b".into(), 1));
        assert!(!pool.add("b".into(), 1));
        assert_eq!(pool.snapshot().len(), 2);
        assert!(pool.remove("a"));
        assert!(!pool.remove("a"));
        assert_eq!(pool.snapshot().len(), 1);
    }

    /// Weighted round robin hands out picks in proportion to weight, and the
    /// schedule is smooth (no long contiguous run of the heavy backend).
    #[test]
    fn weighted_distribution_matches_weights() {
        let pool = BackendPool::new(
            vec![("a".into(), 3), ("b".into(), 1)],
            Strategy::RoundRobin,
        );
        let mut a = 0;
        let mut b = 0;
        for _ in 0..400 {
            match pool.pick().unwrap().addr.as_str() {
                "a" => a += 1,
                "b" => b += 1,
                _ => unreachable!(),
            }
        }
        // exact 3:1 over a whole number of cycles
        assert_eq!(a, 300);
        assert_eq!(b, 100);
    }

    #[test]
    fn set_weight_shifts_traffic() {
        let pool = BackendPool::new(
            vec![("a".into(), 1), ("b".into(), 1)],
            Strategy::RoundRobin,
        );
        // drain b entirely -> all traffic to a
        assert!(pool.set_weight("b", 0));
        for _ in 0..10 {
            assert_eq!(pool.pick().unwrap().addr, "a");
        }
        assert!(!pool.set_weight("missing", 5));
    }

    #[test]
    fn weight_zero_backend_is_drained_but_still_listed() {
        let pool = BackendPool::new(
            vec![("a".into(), 0), ("b".into(), 1)],
            Strategy::RoundRobin,
        );
        // a is still a member (shows up in snapshot / admin list) but takes no traffic
        assert_eq!(pool.snapshot().len(), 2);
        for _ in 0..10 {
            assert_eq!(pool.pick().unwrap().addr, "b");
        }
    }

    #[test]
    fn schedule_is_smooth_not_bursty() {
        // 5:1 should interleave, not emit aaaaa then b
        let sched = build_schedule(&[5, 1]);
        assert_eq!(sched.len(), 6);
        // the single b must not sit at either end of a 5-long run of a
        let b_pos = sched.iter().position(|&i| i == 1).unwrap();
        assert!(b_pos > 0 && b_pos < 5, "b landed at {b_pos}, schedule not smooth");
    }
}
