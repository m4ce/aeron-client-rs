use std::marker::PhantomData;
use std::slice;

pub(super) unsafe extern "C" fn fragment_handler_trampoline<T: FragmentHandler>(
    clientd: *mut std::os::raw::c_void,
    buffer: *const u8,
    length: usize,
    header: *mut libaeron_sys::aeron_header_t,
) {
    // trampoline
    let handler = clientd as *mut T;
    // copy the header values
    let mut header_values = libaeron_sys::aeron_header_values_t{
        frame: libaeron_sys::aeron_header_values_frame_t{
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
    };
    if libaeron_sys::aeron_header_values(header, &mut header_values) < 0 {
        panic!("aeron_header_values: failed to retrieve headers");
    }
    (*handler).on_fragment(slice::from_raw_parts(buffer, length), &header_values);
}

// enable this once trait aliases are in stable - https://github.com/rust-lang/rust/issues/41517
// pub trait FragmentHandler = FnMut(&[u8], &libaeron_sys::aeron_header_t);
pub trait FragmentHandler {
    fn on_fragment(&mut self, _data: &[u8], _header: &libaeron_sys::aeron_header_values_t);
}

pub trait FragmentProcessor {
    fn handler(&self) -> libaeron_sys::aeron_fragment_handler_t;

    fn user_data(&self) -> *mut std::os::raw::c_void;
}

pub struct DefaultFragmentProcessor<T> {
    handler_ptr: *mut std::os::raw::c_void,
    phantom: PhantomData<T>
}

impl <T> DefaultFragmentProcessor<T> {
    pub fn new(mut handler: &T) -> Self where T: FragmentHandler {
        DefaultFragmentProcessor {
            handler_ptr: &mut handler as *mut _ as *mut std::os::raw::c_void,
            phantom: PhantomData
        }
    }
}

impl <T: FragmentHandler> FragmentProcessor for DefaultFragmentProcessor<T> {
    fn handler(&self) -> libaeron_sys::aeron_fragment_handler_t {
        Some(fragment_handler_trampoline::<T>)
    }

    fn user_data(&self) -> *mut std::os::raw::c_void {
        self.handler_ptr
    }
}