use anyhow::bail;

pub struct Header {
    ptr: *const libaeron_sys::aeron_header_t
}

impl Header {
    pub(super) fn new(ptr: *const libaeron_sys::aeron_header_t) -> Self {
        Self { ptr }
    }

    pub fn frame_length(&self) -> i32 {
        unsafe {
            (*(*self.ptr).frame).frame_header.frame_length
        }
    }

    pub fn version(&self) -> i8 {
        unsafe {
            (*(*self.ptr).frame).frame_header.version
        }
    }

    pub fn flags(&self) -> u8 {
        unsafe {
            (*(*self.ptr).frame).frame_header.flags
        }
    }

    pub fn header_type(&self) -> i16 {
        unsafe {
            (*(*self.ptr).frame).frame_header.type_
        }
    }

    pub fn term_offset(&self) -> i32 {
        unsafe {
            (*(*self.ptr).frame).term_offset
        }
    }

    pub fn session_id(&self) -> i32 {
        unsafe {
            (*(*self.ptr).frame).session_id
        }
    }

    pub fn stream_id(&self) -> i32 {
        unsafe {
            (*(*self.ptr).frame).stream_id
        }
    }

    pub fn term_id(&self) -> i32 {
        unsafe {
            (*(*self.ptr).frame).term_id
        }
    }

    pub fn reserved_value(&self) -> i64 {
        unsafe {
            (*(*self.ptr).frame).reserved_value
        }
    }

    pub fn initial_term_id(&self) -> i32 {
        unsafe {
            (*self.ptr).initial_term_id
        }
    }

    pub fn position_bits_to_shift(&self) -> usize {
        unsafe {
            (*self.ptr).position_bits_to_shift
        }
    }
}