use crate::BufferQueue;
use crate::{
    PopStrategy, PushStrategy,
    buffer_modes::{MPMC, MPSC, SPMC, SPSC},
};

impl<T, const N: usize> PushStrategy<T, N> for SPSC {
    #[inline(always)]
    fn push(q: &BufferQueue<T, Self, N>, val: T) -> bool {
        q.raw_sp_push(val)
    }
}
impl<T, const N: usize> PushStrategy<T, N> for SPMC {
    #[inline(always)]
    fn push(q: &BufferQueue<T, Self, N>, val: T) -> bool {
        q.raw_sp_push(val)
    }
}
impl<T, const N: usize> PushStrategy<T, N> for MPSC {
    #[inline(always)]
    fn push(q: &BufferQueue<T, Self, N>, val: T) -> bool {
        q.raw_mp_push(val)
    }
}
impl<T, const N: usize> PushStrategy<T, N> for MPMC {
    #[inline(always)]
    fn push(q: &BufferQueue<T, Self, N>, val: T) -> bool {
        q.raw_mp_push(val)
    }
}

impl<T, const N: usize> PopStrategy<T, N> for SPSC {
    #[inline(always)]
    fn pop(q: &BufferQueue<T, Self, N>) -> Option<T> {
        q.raw_sc_pop()
    }
}
impl<T, const N: usize> PopStrategy<T, N> for MPSC {
    #[inline(always)]
    fn pop(q: &BufferQueue<T, Self, N>) -> Option<T> {
        q.raw_sc_pop()
    }
}
impl<T, const N: usize> PopStrategy<T, N> for SPMC {
    #[inline(always)]
    fn pop(q: &BufferQueue<T, Self, N>) -> Option<T> {
        q.raw_mc_pop()
    }
}
impl<T, const N: usize> PopStrategy<T, N> for MPMC {
    #[inline(always)]
    fn pop(q: &BufferQueue<T, Self, N>) -> Option<T> {
        q.raw_mc_pop()
    }
}
