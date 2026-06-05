use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};

use crate::{BufferMode, PopStrategy, PushStrategy};

pub struct BufferQueue<T, M: BufferMode, const N: usize> {
    pub tail: AtomicU8,
    pub head: AtomicU8,
    pub invalidated: AtomicBool,
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
            invalidated: AtomicBool::new(false),
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
        // Formally set the flag to shut down active operations across threads
        self.invalidated.store(true, Ordering::Release);

        // Read pointers after flagging to drop remaining elements cleanly
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        let ready_mask = self.ready.load(Ordering::Acquire);
        let mask = (N - 1) as u8;

        let count = tail.wrapping_sub(head);
        for i in 0..count {
            let idx = ((head.wrapping_add(i)) & mask) as usize;
            // Only drop the item if the producer actually finished writing it
            if (ready_mask & (1 << idx)) != 0 {
                unsafe {
                    let ptr = self.buf.get() as *mut MaybeUninit<T>;
                    std::ptr::drop_in_place(ptr.add(idx) as *mut T);
                }
            }
        }
    }

    #[inline]
    pub fn is_invalidated(&self) -> bool {
        self.invalidated.load(Ordering::Acquire)
    }
}

impl<T, M: BufferMode, const N: usize> Drop for BufferQueue<T, M, N> {
    #[inline]
    fn drop(&mut self) {
        // Direct mutable access bypasses atomic overhead during drop
        if *self.invalidated.get_mut() {
            return;
        }

        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut();
        let ready_mask = *self.ready.get_mut();
        let mask = (N - 1) as u8;

        let count = tail.wrapping_sub(head);
        for i in 0..count {
            let idx = ((head.wrapping_add(i)) & mask) as usize;
            if (ready_mask & (1 << idx)) != 0 {
                unsafe {
                    let ptr = self.buf.get() as *mut MaybeUninit<T>;
                    std::ptr::drop_in_place(ptr.add(idx) as *mut T);
                }
            }
        }
    }
}

// Raw operations on different types of Buffers
impl<T, M: BufferMode, const N: usize> BufferQueue<T, M, N> {
    // Single producer loop, directly pushed atomically, no CAS loop
    #[inline(always)]
    pub fn raw_sp_push(&self, value: T) -> bool {
        if self.invalidated.load(Ordering::Acquire) {
            return false;
        }

        let head = self.head.load(Ordering::Acquire);
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

        // Release ordering ensures the data write above is fully committed
        // to memory before the consumer reads the updated flag or pointer.
        self.ready.fetch_or(1 << idx, Ordering::Release);
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        true
    }

    // Multiple producers, perform CAS loop
    #[inline(always)]
    pub fn raw_mp_push(&self, value: T) -> bool {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let mask = (N - 1) as u8;

        loop {
            if self.invalidated.load(Ordering::Relaxed) {
                return false;
            }

            let head = self.head.load(Ordering::Acquire);
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
        if self.invalidated.load(Ordering::Acquire) {
            return None;
        }

        let head = self.head.load(Ordering::Relaxed);
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

        // SAFETY: Release blocks CPU from clearing the bitmask before the item has finished reading.
        self.ready.fetch_and(!(1 << idx), Ordering::Release);
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    // Multiple consumer pop, CAS loop implemented
    #[inline(always)]
    pub fn raw_mc_pop(&self) -> Option<T> {
        let mut head = self.head.load(Ordering::Relaxed);
        let mask = (N - 1) as u8;

        loop {
            if self.invalidated.load(Ordering::Relaxed) {
                return None;
            }

            let tail = self.tail.load(Ordering::Acquire);
            if head == tail {
                return None;
            }

            let idx = (head & mask) as usize;
            if (self.ready.load(Ordering::Acquire) & (1 << idx)) == 0 {
                std::hint::spin_loop();
                continue;
            }

            // SAFETY: AcqRel on success handles tracking the structural modification
            // while making sure previous producer writes are completely synchronized.
            match self.head.compare_exchange_weak(
                head,
                head.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let value = unsafe {
                        (self.buf.get() as *mut MaybeUninit<T>)
                            .add(idx)
                            .read()
                            .assume_init()
                    };
                    self.ready.fetch_and(!(1 << idx), Ordering::Release);
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
