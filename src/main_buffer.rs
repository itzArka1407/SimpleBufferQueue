use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

use crate::{BufferMode, PopStrategy, PushStrategy};

pub const INVALID: u8 = 255; // The tag that is used to set the invalid state

pub struct BufferQueue<T, M: BufferMode, const N: usize> {
    pub tail: AtomicU8,
    pub head: AtomicU8,
    pub ready: AtomicU32,
    pub buf: UnsafeCell<[MaybeUninit<T>; N]>,
    pub _mode: std::marker::PhantomData<M>,
}

unsafe impl<T: Send, M: BufferMode, const N: usize> Send for BufferQueue<T, M, N> {}
unsafe impl<T: Send, M: BufferMode, const N: usize> Sync for BufferQueue<T, M, N> {}

impl<T, M: BufferMode, const N: usize> BufferQueue<T, M, N> {
    #[inline]
    pub fn new() -> Self {
        const {
            assert!(
                N > 0 && (N & (N - 1)) == 0,
                "Capacity must be a power of two"
            );
            assert!(N <= 32, "Capacity cannot exceed 32");
        }
        Self {
            tail: AtomicU8::new(0),
            head: AtomicU8::new(0),
            ready: AtomicU32::new(0),
            buf: UnsafeCell::new(unsafe { MaybeUninit::uninit().assume_init() }),
            _mode: std::marker::PhantomData,
        }
    }

    #[inline(always)]
    pub fn push(&self, value: T) -> bool
    where
        M: PushStrategy<T, N>,
    {
        M::push(self, value)
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T>
    where
        M: PopStrategy<T, N>,
    {
        M::pop(self)
    }

    #[inline]
    pub fn invalidate(&self) {
        // Swap out the head with INVALID atomically -- shuts down opertions
        let old_head = self.head.swap(INVALID, Ordering::AcqRel);

        // If not already invalidated, drop the valid elements
        if old_head != INVALID {
            let tail = self.tail.load(Ordering::Acquire);
            let ready_mask = self.ready.load(Ordering::Acquire);
            let mask = (N - 1) as u8;

            let count = tail.wrapping_sub(old_head);
            for i in 0..count {
                let idx = ((old_head.wrapping_add(i)) & mask) as usize;
                // Only drop the item if the producer actually finished writing it -- indicated by
                // the bit in the mask
                if (ready_mask & (1 << idx)) != 0 {
                    unsafe {
                        let ptr = self.buf.get() as *mut MaybeUninit<T>;
                        std::ptr::drop_in_place(ptr.add(idx) as *mut T);
                    }
                }
            }
        }
    }
    #[inline]
    pub fn is_invalidated(&self) -> bool {
        self.head.load(Ordering::Acquire) == INVALID
    }
}

impl<T, M: BufferMode, const N: usize> Drop for BufferQueue<T, M, N> {
    #[inline]
    fn drop(&mut self) {
        // Direct mutable access bypasses atomic overhead during drop
        let head = *self.head.get_mut();

        // Already invalidated, hence cleared, nothing to clear -- back off
        if head == INVALID {
            return;
        }

        let tail = *self.tail.get_mut();
        let ready_mask = *self.ready.get_mut();
        let mask = (N - 1) as u8;

        let count = tail.wrapping_sub(head);
        for i in 0..count {
            let idx = ((head.wrapping_add(i)) & mask) as usize;
            // Only drop the item if it was fully written and marked ready
            if (ready_mask & (1 << idx)) != 0 {
                unsafe {
                    let ptr = self.buf.get() as *mut MaybeUninit<T>;
                    std::ptr::drop_in_place(ptr.add(idx) as *mut T);
                }
            }
        }
    }
}

// The raw operations on different types of Buffers are implemented here
impl<T, M: BufferMode, const N: usize> BufferQueue<T, M, N> {
    // Single producer loop, directly pushed atomically, no CAS loop
    #[inline(always)]
    pub fn raw_sp_push(&self, value: T) -> bool {
        let head = self.head.load(Ordering::Acquire);
        if head == INVALID {
            return false;
        }
        let tail = self.tail.load(Ordering::Relaxed);
        if tail.wrapping_sub(head) >= N as u8 {
            return false;
        }

        let mask = (N - 1) as u8;
        let idx = (tail & mask) as usize;
        unsafe {
            (self.buf.get() as *mut MaybeUninit<T>)
                .add(idx)
                .write(MaybeUninit::new(value));
        }

        self.ready.fetch_or(1 << idx, Ordering::Release);
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        true
    }

    // Multiple producers, perform CAS loop, laggier than SP push but works
    #[inline(always)]
    pub fn raw_mp_push(&self, value: T) -> bool {
        if self.head.load(Ordering::Acquire) == INVALID {
            return false;
        }
        let mut tail = self.tail.load(Ordering::Relaxed);
        let mask = (N - 1) as u8;
        loop {
            let head = self.head.load(Ordering::Acquire);
            if head == INVALID {
                return false;
            }
            if tail.wrapping_sub(head) >= N as u8 {
                return false;
            }

            match self.tail.compare_exchange_weak(
                tail,
                tail.wrapping_add(1),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let idx = (tail & mask) as usize;
                    unsafe {
                        (self.buf.get() as *mut MaybeUninit<T>)
                            .add(idx)
                            .write(MaybeUninit::new(value));
                    }
                    self.ready.fetch_or(1 << idx, Ordering::Release);
                    return true;
                }
                Err(new_tail) => {
                    tail = new_tail;
                    continue;
                }
            }
        }
    }

    // Single consumer pop -- direct, no CAS loop
    #[inline(always)]
    pub fn raw_sc_pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        if head == INVALID {
            return None;
        }
        let tail = self.tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }

        let mask = (N - 1) as u8;
        let idx = (head & mask) as usize;
        if (self.ready.load(Ordering::Acquire) & (1 << idx)) == 0 {
            return None;
        }

        let value = unsafe {
            (self.buf.get() as *mut MaybeUninit<T>)
                .add(idx)
                .read()
                .assume_init()
        };
        self.ready.fetch_and(!(1 << idx), Ordering::Release);
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    // Multiple consumer pop, CAS loop implemented -- more laggy
    #[inline(always)]
    pub fn raw_mc_pop(&self) -> Option<T> {
        let mut head = self.head.load(Ordering::Relaxed);
        let mask = (N - 1) as u8;
        loop {
            if head == INVALID {
                return None;
            }
            let tail = self.tail.load(Ordering::Acquire);
            let idx = (head & mask) as usize;
            if head == tail {
                return None;
            }

            if (self.ready.load(Ordering::Acquire) & (1 << idx)) == 0 {
                std::hint::spin_loop();
                continue;
            }

            match self.head.compare_exchange_weak(
                head,
                head.wrapping_add(1),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let value = unsafe {
                        (self.buf.get() as *mut MaybeUninit<T>)
                            .add(idx)
                            .read()
                            .assume_init()
                    };
                    self.ready.fetch_and(!(1 << idx), Ordering::Relaxed);
                    return Some(value);
                }
                Err(new_head) => {
                    head = new_head;
                    continue;
                }
            }
        }
    }
}
