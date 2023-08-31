use std::ptr::null_mut;

pub trait DestinationReadiness {
    fn ready(ptr: *mut libaeron_sys::aeron_async_destination_t) -> anyhow::Result<bool>;
}

pub struct Destination {
    ptr: *mut libaeron_sys::aeron_async_destination_t,
    completed: bool
}

impl Destination {
    pub(super) fn new() -> Self {
        Self {
            ptr: null_mut(),
            completed: false
        }
    }

    pub(super) fn handle(&self) -> *mut libaeron_sys::aeron_async_destination_t {
        self.ptr
    }

    pub fn poll_ready<T>(&mut self) -> anyhow::Result<bool>
        where
            T: DestinationReadiness,
    {
        if self.completed {
            return Ok(true);
        }
        self.completed = T::ready(self.ptr)?;
        Ok(self.completed)
    }
}