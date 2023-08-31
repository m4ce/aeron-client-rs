use std::ffi::CStr;
use std::ptr::null_mut;
use anyhow::bail;
use crate::fragment_processor::{fragment_handler_trampoline, FragmentHandler, FragmentProcessor};

pub struct FragmentAssemblerProcessor {
    inner: libaeron_sys::aeron_fragment_handler_t,
    clientd: *mut libaeron_sys::aeron_fragment_assembler_t
}

impl FragmentProcessor for FragmentAssemblerProcessor {
    fn handler(&self) -> libaeron_sys::aeron_fragment_handler_t {
        self.inner
    }

    fn user_data(&self) -> *mut std::os::raw::c_void {
        self.clientd as *mut std::os::raw::c_void
    }
}

pub struct FragmentAssembler {
    inner: *mut libaeron_sys::aeron_fragment_assembler_t
}

impl FragmentAssembler {
    pub fn new<T>(mut handler: &T) -> anyhow::Result<Self>
        where
            T: FragmentHandler,
    {
        let mut instance = Self { inner: null_mut() };

        unsafe {
            if libaeron_sys::aeron_fragment_assembler_create(
                &mut instance.inner,
                Some(fragment_handler_trampoline::<T>),
                &mut handler as *mut _ as *mut std::os::raw::c_void
            ) < 0
            {
                bail!(format!(
                    "aeron_fragment_assembler_create: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(instance)
    }

    pub fn processor(&self) -> FragmentAssemblerProcessor {
        FragmentAssemblerProcessor {
            clientd: self.inner,
            inner: Some(libaeron_sys::aeron_fragment_assembler_handler)
        }
    }
}

impl Drop for FragmentAssembler {
    fn drop(&mut self) {
        unsafe {
            libaeron_sys::aeron_fragment_assembler_delete(self.inner);
        }
    }
}