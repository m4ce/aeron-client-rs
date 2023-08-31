use std::ffi::CStr;
use std::ptr::null_mut;
use anyhow::bail;
use crate::destination::{Destination, DestinationReadiness};
use crate::fragment_processor::FragmentProcessor;
use crate::image::Image;

unsafe extern "C" fn image_handler<T: Fn(&Image)>(image: *mut libaeron_sys::aeron_image_t, clientd: *mut std::os::raw::c_void) {
    // trampoline
    let handler = clientd as *mut T;
    (*handler)(&Image::new(image, null_mut()));
}

struct SubscriptionAsyncDestination {}

impl DestinationReadiness for SubscriptionAsyncDestination {
    fn ready(async_: *mut libaeron_sys::aeron_async_destination_t) -> anyhow::Result<bool> {
        unsafe {
            match libaeron_sys::aeron_subscription_async_destination_poll(async_) {
                0 => Ok(false),
                1 => Ok(true),
                _ => bail!(format!(
                    "aeron_subscription_async_destination_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }
}

pub struct Subscription {
    channel: String,
    async_ptr: *mut libaeron_sys::aeron_async_add_subscription_t,
    ptr: *mut libaeron_sys::aeron_subscription_t,
    client_ptr: *mut libaeron_sys::aeron_t,
}

impl Subscription {
    pub(super) fn new(channel: String, client_ptr: *mut libaeron_sys::aeron_t) -> Self {
        Self {
            channel,
            async_ptr: null_mut(),
            ptr: null_mut(),
            client_ptr
        }
    }

    pub(super) fn async_mut_ptr(&mut self) -> *mut *mut libaeron_sys::aeron_async_add_subscription_t {
        &mut self.async_ptr
    }

    pub(super) fn async_ptr(&self) -> *mut libaeron_sys::aeron_async_add_subscription_t {
        self.async_ptr
    }

    pub fn channel(&self) -> &str {
        self.channel.as_str()
    }

    pub fn poll_ready(&mut self) -> anyhow::Result<bool> {
        if !self.ptr.is_null() {
            return Ok(true);
        }
        unsafe {
            match libaeron_sys::aeron_async_add_subscription_poll(&mut self.ptr, self.async_ptr) {
                0 => Ok(false),
                1 => Ok(!self.ptr.is_null()),
                _ => bail!(format!(
                    "aeron_async_add_subscription_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }

    pub fn channel_status(&self) -> i64 {
        unsafe { libaeron_sys::aeron_subscription_channel_status(self.ptr) }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { libaeron_sys::aeron_subscription_is_connected(self.ptr) }
    }

    pub fn async_add_destination(
        &self,
        endpoint_channel: String,
    ) -> anyhow::Result<Destination> {
        let mut async_destination: Destination = Destination::new();
        unsafe {
            if libaeron_sys::aeron_subscription_async_add_destination(
                &mut async_destination.handle(),
                self.client_ptr,
                self.ptr,
                endpoint_channel.as_ptr() as *const std::os::raw::c_char
            ) < 0
            {
                bail!(format!(
                    "aeron_subscription_async_add_destination: {:?}",
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
            if libaeron_sys::aeron_subscription_async_remove_destination(
                &mut async_destination.handle(),
                self.client_ptr,
                self.ptr,
                endpoint_channel.as_ptr() as *const std::os::raw::c_char
            ) < 0
            {
                bail!(format!(
                    "aeron_subscription_async_remove_destination: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(async_destination)
    }

    pub fn image_at_index(&self, index: usize) -> anyhow::Result<Image> {
        unsafe {
            let ptr = libaeron_sys::aeron_subscription_image_at_index(self.ptr, index);
            if ptr.is_null() {
                bail!(format!("No image exists at index {}", index));
            }
            Ok(Image::new(ptr, self.ptr))
        }
    }

    pub fn image_count(&self) -> i32 {
        unsafe { libaeron_sys::aeron_subscription_image_count(self.ptr) }
    }

    pub fn image_by_session_id(&self, session_id: i32) -> Option<Image> {
        unsafe {
            let ptr = libaeron_sys::aeron_subscription_image_by_session_id(self.ptr, session_id);
            if ptr.is_null() {
                None
            } else {
                Some(Image::new(ptr, self.ptr))
            }
        }
    }

    pub fn for_each_image<T>(&self, mut handler: &T) where T: Fn(&Image) {
        unsafe {
            libaeron_sys::aeron_subscription_for_each_image(self.ptr,
                                                            Some(image_handler::<T>),
                                                            &mut handler as *mut _ as *mut std::os::raw::c_void);
        }
    }

    pub fn poll<T>(&self, fragment_processor: &T, fragment_limit: usize) -> anyhow::Result<i32>
        where
            T: FragmentProcessor,
    {
        unsafe {
            match libaeron_sys::aeron_subscription_poll(
                self.ptr,
                fragment_processor.handler(),
                fragment_processor.user_data(),
                fragment_limit,
            ) {
                -1 => bail!(format!(
                    "aeron_subscription_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
                work => Ok(work),
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        unsafe { libaeron_sys::aeron_subscription_is_closed(self.ptr) }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        unsafe {
            libaeron_sys::aeron_subscription_close(self.ptr, None, null_mut());
        }
    }
}
