use anyhow::bail;

pub struct Header {
    inner: libaeron_sys::aeron_header_values_t
}

impl Header {
    pub(super) fn new(ptr: *mut libaeron_sys::aeron_header_t) -> anyhow::Result<Self> {
        let mut instance = Self {
            inner: libaeron_sys::aeron_header_values_t {
                frame: libaeron_sys::aeron_header_values_frame_t {
                    frame_length: 0,
                    version: 0,
                    flags: 0,
                    type_: 0,
                    term_offset: 0,
                    session_id: 0,
                    stream_id: 0,
                    term_id: 0,
                    reserved_value: 0,
                },
                initial_term_id: 0,
                position_bits_to_shift: 0
            }
        };
        unsafe {
            if libaeron_sys::aeron_header_values(ptr, &mut instance.inner) < 0 {
                bail!("aeron_header_values: failed to retrieve header values");
            }
        }
        Ok(instance)
    }

    pub fn frame_length(&self) -> i32 {
        self.inner.frame.frame_length
    }

    pub fn version(&self) -> i8 {
        self.inner.frame.version
    }

    pub fn flags(&self) -> u8 {
        self.inner.frame.flags
    }

    pub fn header_type(&self) -> i16 {
        self.inner.frame.type_
    }

    pub fn term_offset(&self) -> i32 {
        self.inner.frame.term_offset
    }

    pub fn session_id(&self) -> i32 {
        self.inner.frame.session_id
    }

    pub fn stream_id(&self) -> i32 {
        self.inner.frame.stream_id
    }

    pub fn term_id(&self) -> i32 {
        self.inner.frame.term_id
    }

    pub fn reserved_value(&self) -> i64 {
        self.inner.frame.reserved_value
    }

    pub fn initial_term_id(&self) -> i32 {
        self.inner.initial_term_id
    }

    pub fn position_bits_to_shift(&self) -> usize {
        self.inner.position_bits_to_shift
    }
}