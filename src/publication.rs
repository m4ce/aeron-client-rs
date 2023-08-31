use std::ffi::CStr;
use std::ptr::null_mut;
use anyhow::bail;
use crate::buffer_claim::BufferClaim;
use crate::destination::{Destination, DestinationReadiness};

struct PublicationAsyncDestination {}

impl DestinationReadiness for PublicationAsyncDestination {
    fn ready(async_: *mut libaeron_sys::aeron_async_destination_t) -> anyhow::Result<bool> {
        unsafe {
            match libaeron_sys::aeron_publication_async_destination_poll(async_) {
                0 => Ok(false),
                1 => Ok(true),
                _ => bail!(format!(
                    "aeron_publication_async_destination_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }
}

pub struct Publication {
    channel: String,
    async_ptr: *mut libaeron_sys::aeron_async_add_publication_t,
    ptr: *mut libaeron_sys::aeron_publication_t,
    client_ptr: *mut libaeron_sys::aeron_t
}

impl Publication {
    pub(super) fn new(channel: String, client_ptr: *mut libaeron_sys::aeron_t) -> Self {
        Self {
            channel,
            async_ptr: null_mut(),
            ptr: null_mut(),
            client_ptr
        }
    }

    pub(super) fn async_mut_ptr(&mut self) -> *mut *mut libaeron_sys::aeron_async_add_publication_t {
        &mut self.async_ptr
    }

    pub(super) fn async_ptr(&mut self) -> *mut libaeron_sys::aeron_async_add_publication_t {
        self.async_ptr
    }

    pub fn poll_ready(&mut self) -> anyhow::Result<bool> {
        if !self.ptr.is_null() {
            return Ok(true);
        }
        unsafe {
            match libaeron_sys::aeron_async_add_publication_poll(&mut self.ptr, self.async_ptr) {
                0 => Ok(false),
                1 => Ok(!self.ptr.is_null()),
                _ => bail!(format!(
                    "aeron_async_add_publication_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }

    pub fn channel_status(&self) -> i64 {
        unsafe { libaeron_sys::aeron_publication_channel_status(self.ptr) }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { libaeron_sys::aeron_publication_is_connected(self.ptr) }
    }

    pub fn channel(&self) -> &str {
        self.channel.as_str()
    }

    pub fn stream_id(&self) -> i32 {
        unsafe { libaeron_sys::aeron_publication_stream_id(self.ptr) }
    }

    pub fn session_id(&self) -> i32 {
        unsafe { libaeron_sys::aeron_publication_session_id(self.ptr) }
    }

    pub fn offer(&self, data: &[u8]) -> anyhow::Result<()> {
        unsafe {
            if libaeron_sys::aeron_publication_offer(
                self.ptr,
                data.as_ptr(),
                data.len(),
                None,
                null_mut(),
            ) < 0
            {
                bail!(format!(
                    "aeron_publication_offer: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(())
    }

    pub fn try_claim(&self, length: usize) -> anyhow::Result<BufferClaim> {
        let mut claim = BufferClaim::new();
        unsafe {
            if libaeron_sys::aeron_publication_try_claim(self.ptr, length, claim.claim()) < 0 {
                bail!(format!(
                    "aeron_publication_try_claim: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(claim)
    }

    pub fn async_add_destination(
        &self,
        endpoint_channel: String,
    ) -> anyhow::Result<Destination> {
        let mut async_destination: Destination = Destination::new();
        unsafe {
            if libaeron_sys::aeron_publication_async_add_destination(
                &mut async_destination.handle(),
                self.client_ptr,
                self.ptr,
                endpoint_channel.as_ptr() as *const std::os::raw::c_char,
            ) < 0
            {
                bail!(format!(
                    "aeron_publication_async_add_destination: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(async_destination)
    }

    pub fn async_remove_destination(
        &self,
        endpoint_channel: String,
    ) -> anyhow::Result<Destination> {
        let mut async_destination: Destination = Destination::new();
        unsafe {
            if libaeron_sys::aeron_publication_async_remove_destination(
                &mut async_destination.handle(),
                self.client_ptr,
                self.ptr,
                endpoint_channel.as_ptr() as *const std::os::raw::c_char,
            ) < 0
            {
                bail!(format!(
                    "aeron_publication_async_remove_destination: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(async_destination)
    }

    pub fn is_closed(&self) -> bool {
        unsafe { libaeron_sys::aeron_publication_is_closed(self.ptr) }
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        unsafe {
            libaeron_sys::aeron_publication_close(self.ptr, None, null_mut());
        }
    }
}