use std::ptr::null_mut;
use anyhow::bail;
use std::ffi::{CStr};
use crate::buffer_claim::BufferClaim;
use crate::destination::{Destination, DestinationReadiness};
use crate::publication::Error;
use crate::publication::Error::{AdminAction, BackPressured, Closed, GenericError, MaxPositionExceeded, NotConnected};

struct ExclusivePublicationAsyncDestination {}

impl DestinationReadiness for ExclusivePublicationAsyncDestination {
    fn ready(ptr: *mut libaeron_sys::aeron_async_destination_t) -> anyhow::Result<bool> {
        unsafe {
            match libaeron_sys::aeron_exclusive_publication_async_destination_poll(ptr) {
                0 => Ok(false),
                1 => Ok(true),
                _ => bail!(format!(
                    "aeron_exclusive_publication_async_destination_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }
}

pub struct ExclusivePublication {
    channel: String,
    async_ptr: *mut libaeron_sys::aeron_async_add_exclusive_publication_t,
    ptr: *mut libaeron_sys::aeron_exclusive_publication_t,
    client_ptr: *mut libaeron_sys::aeron_t
}

impl ExclusivePublication {
    pub(super) fn new(channel: String, client_ptr: *mut libaeron_sys::aeron_t) -> Self {
        Self {
            channel,
            async_ptr: null_mut(),
            ptr: null_mut(),
            client_ptr
        }
    }

    pub(super) fn async_mut_ptr(&mut self) -> *mut *mut libaeron_sys::aeron_async_add_exclusive_publication_t {
        &mut self.async_ptr
    }

    pub(super) fn async_ptr(&self) -> *mut libaeron_sys::aeron_async_add_exclusive_publication_t {
        self.async_ptr
    }

    pub fn poll_ready(&mut self) -> anyhow::Result<bool> {
        if !self.ptr.is_null() {
            return Ok(true);
        }
        unsafe {
            match libaeron_sys::aeron_async_add_exclusive_publication_poll(&mut self.ptr, self.async_ptr) {
                0 => Ok(false),
                1 => Ok(!self.ptr.is_null()),
                _ => bail!(format!(
                    "aeron_async_add_exclusive_publication_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }

    pub fn channel_status(&self) -> i64 {
        unsafe { libaeron_sys::aeron_exclusive_publication_channel_status(self.ptr) }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { libaeron_sys::aeron_exclusive_publication_is_connected(self.ptr) }
    }

    pub fn channel(&self) -> &str {
        self.channel.as_str()
    }

    pub fn stream_id(&self) -> i32 {
        unsafe { libaeron_sys::aeron_exclusive_publication_stream_id(self.ptr) }
    }

    pub fn session_id(&self) -> i32 {
        unsafe { libaeron_sys::aeron_exclusive_publication_session_id(self.ptr) }
    }

    pub fn offer(&self, data: &[u8]) -> Result<(), Error> {
        unsafe {
            let pos = libaeron_sys::aeron_exclusive_publication_offer(
                self.ptr,
                data.as_ptr(),
                data.len(),
                None,
                null_mut(),
            );
            if pos >= 0 {
                Ok(())
            } else {
                match pos as i32 {
                    libaeron_sys::AERON_PUBLICATION_NOT_CONNECTED => Err(NotConnected),
                    libaeron_sys::AERON_PUBLICATION_ADMIN_ACTION => Err(AdminAction),
                    libaeron_sys::AERON_PUBLICATION_BACK_PRESSURED => Err(BackPressured),
                    libaeron_sys::AERON_PUBLICATION_CLOSED => Err(Closed),
                    libaeron_sys::AERON_PUBLICATION_MAX_POSITION_EXCEEDED => Err(MaxPositionExceeded),
                    _ => Err(GenericError(CStr::from_ptr(libaeron_sys::aeron_errmsg())))
                }
            }
        }
    }

    pub fn try_claim(&self, length: usize) -> Result<BufferClaim, Error> {
        let mut claim = BufferClaim::new();
        unsafe {
            let pos = libaeron_sys::aeron_exclusive_publication_try_claim(self.ptr, length, claim.claim());
            if pos >= 0 {
                Ok(claim)
            } else {
                match pos as i32 {
                    libaeron_sys::AERON_PUBLICATION_NOT_CONNECTED => Err(NotConnected),
                    libaeron_sys::AERON_PUBLICATION_ADMIN_ACTION => Err(AdminAction),
                    libaeron_sys::AERON_PUBLICATION_BACK_PRESSURED => Err(BackPressured),
                    libaeron_sys::AERON_PUBLICATION_CLOSED => Err(Closed),
                    libaeron_sys::AERON_PUBLICATION_MAX_POSITION_EXCEEDED => Err(MaxPositionExceeded),
                    _ => Err(GenericError(CStr::from_ptr(libaeron_sys::aeron_errmsg())))
                }
            }
        }
    }

    pub fn async_add_destination(
        &self,
        endpoint_channel: String,
    ) -> anyhow::Result<Destination> {
        let mut async_destination: Destination = Destination::new();
        unsafe {
            if libaeron_sys::aeron_exclusive_publication_async_add_destination(
                &mut async_destination.handle(),
                self.client_ptr,
                self.ptr,
                endpoint_channel.as_ptr() as *const std::os::raw::c_char,
            ) < 0
            {
                bail!(format!(
                    "aeron_exclusive_publication_async_add_destination: {:?}",
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
            if libaeron_sys::aeron_exclusive_publication_async_remove_destination(
                &mut async_destination.handle(),
                self.client_ptr,
                self.ptr,
                endpoint_channel.as_ptr() as *const std::os::raw::c_char,
            ) < 0
            {
                bail!(format!(
                    "aeron_exclusive_publication_async_remove_destination: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(async_destination)
    }

    pub fn is_closed(&self) -> bool {
        unsafe { libaeron_sys::aeron_exclusive_publication_is_closed(self.ptr) }
    }
}

impl Drop for ExclusivePublication {
    fn drop(&mut self) {
        unsafe {
            libaeron_sys::aeron_exclusive_publication_close(self.ptr, None, null_mut());
        }
    }
}