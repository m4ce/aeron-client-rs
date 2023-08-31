pub struct Image {
    ptr: *mut libaeron_sys::aeron_image_t,
}

impl Image {
    pub(super) fn wrap(ptr: *mut libaeron_sys::aeron_image_t) -> Self {
        Self { ptr }
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