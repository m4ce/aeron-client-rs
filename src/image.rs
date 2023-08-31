pub struct Image {
    ptr: *mut libaeron_sys::aeron_image_t,
    subscription_ptr: *mut libaeron_sys::aeron_subscription_t
}

impl Image {
    pub(super) fn new(ptr: *mut libaeron_sys::aeron_image_t, subscription_ptr: *mut libaeron_sys::aeron_subscription_t) -> Self {
        Self { ptr, subscription_ptr }
    }

    pub fn session_id(&self) -> i32 {
        unsafe {
            libaeron_sys::aeron_image_session_id(self.ptr)
        }
    }

    pub fn is_eof(&self) -> bool {
        unsafe {
            libaeron_sys::aeron_image_is_end_of_stream(self.ptr)
        }
    }

    pub fn eof_position(&self) -> i64 {
        unsafe {
            libaeron_sys::aeron_image_end_of_stream_position(self.ptr)
        }
    }

    pub fn is_closed(&self) -> bool {
        unsafe {
            libaeron_sys::aeron_image_is_closed(self.ptr)
        }
    }
}

impl Drop for Image {
    fn drop(&mut self) {
        unsafe {
            if !self.subscription_ptr.is_null() {
                libaeron_sys::aeron_subscription_image_release(self.subscription_ptr, self.ptr);
            }
        }
    }
}