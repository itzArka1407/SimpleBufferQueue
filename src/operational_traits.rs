use crate::BufferQueue;

// Any form of buffer implements this
pub trait BufferMode {}

pub trait PushStrategy<T, const N: usize>: Sized {
    fn push(queue: &BufferQueue<T, Self, N>, value: T) -> bool
    where
        Self: BufferMode;
}

pub trait PopStrategy<T, const N: usize>: Sized {
    fn pop(queue: &BufferQueue<T, Self, N>) -> Option<T>
    where
        Self: BufferMode;
}
