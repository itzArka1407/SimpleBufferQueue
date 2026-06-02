use crate::BufferMode;

pub struct SPSC;
pub struct MPSC;
pub struct SPMC;
pub struct MPMC;

impl BufferMode for SPSC {}
impl BufferMode for MPSC {}
impl BufferMode for MPMC {}
impl BufferMode for SPMC {}
