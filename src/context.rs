use std::ffi::CStr;
use std::ptr::null_mut;
use anyhow::bail;

unsafe extern "C" fn error_handler_trampoline<T: ErrorHandler>(clientd: *mut ::std::os::raw::c_void, errcode: std::os::raw::c_int, message: *const ::std::os::raw::c_char) {
    let handler = clientd as *mut T;
    (*handler).on_error(errcode, CStr::from_ptr(message));
}

unsafe extern "C" fn on_new_subscription_handler_trampoline<T: OnNewSubscriptionHandler>(clientd: *mut std::os::raw::c_void, async_: *mut libaeron_sys::aeron_async_add_subscription_t, channel: *const std::os::raw::c_char, stream_id: i32, correlation_id: i64) {
    let handler = clientd as *mut T;
    (*handler).handle(CStr::from_ptr(channel), stream_id, correlation_id);
}

unsafe extern "C" fn on_new_publication_handler_trampoline<T: OnNewPublicationHandler>(clientd: *mut ::std::os::raw::c_void, async_: *mut libaeron_sys::aeron_async_add_publication_t, channel: *const ::std::os::raw::c_char, stream_id: i32, session_id: i32, correlation_id: i64) {
    let handler = clientd as *mut T;
    (*handler).handle(CStr::from_ptr(channel), stream_id, session_id, correlation_id);
}

pub trait ErrorHandler {
    fn on_error(&self, code: i32, msg: &CStr);
}

pub trait OnNewSubscriptionHandler {
    fn handle(&self, channel: &CStr, stream_id: i32, correlation_id: i64);
}

pub trait OnNewPublicationHandler {
    fn handle(&self, channel: &CStr, stream_id: i32, session_id: i32, correlation_id: i64);
}

pub struct Context {
    ptr: *mut libaeron_sys::aeron_context_t,
    directory: String
}

impl Context {
    #[cfg(target_os = "linux")]
    pub const DEFAULT_AERON_DIRECTORY: &'static str = "/dev/shm/aeron";
    #[cfg(target_os = "macos")]
    pub const DEFAULT_AERON_DIRECTORY: &'static str = "/Volumes/DevShm/aeron";

    pub fn new() -> anyhow::Result<Self> {
        let mut context = Self {
            ptr: null_mut(),
            directory: "".into()
        };
        unsafe {
            if libaeron_sys::aeron_context_init(&mut context.ptr) < 0 {
                bail!(format!(
                    "aeron_context_init: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        context.set_dir(Context::DEFAULT_AERON_DIRECTORY.into())?;
        Ok(context)
    }

    pub(super) fn ptr(&self) -> *mut libaeron_sys::aeron_context_t {
        self.ptr
    }

    pub fn set_use_conductor_agent_invoker(&mut self, value: bool) -> anyhow::Result<()> {
        unsafe {
            if libaeron_sys::aeron_context_set_use_conductor_agent_invoker(self.ptr, value) < 0 {
                bail!(format!(
                    "aeron_context_set_use_conductor_agent_invoker: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            Ok(())
        }
    }

    pub fn set_dir(&mut self, dir: String) -> anyhow::Result<()> {
        self.directory = dir;
        unsafe {
            if libaeron_sys::aeron_context_set_dir(self.ptr, self.directory.as_ptr() as *const std::os::raw::c_char) < 0
            {
                bail!(format!(
                    "aeron_context_set_dir: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            Ok(())
        }
    }

    pub fn set_error_handler<T>(&mut self, mut handler: &T) -> anyhow::Result<()> where T: ErrorHandler {
        unsafe {
            if libaeron_sys::aeron_context_set_error_handler(
                self.ptr,
                Some(error_handler_trampoline::<T>),
                &mut handler as *mut _ as *mut std::os::raw::c_void
            ) < 0
            {
                bail!(format!(
                    "aeron_context_set_error_handler: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            Ok(())
        }
    }

    pub fn set_new_subscription_handler<T>(
        &self,
        mut handler: &T
    ) -> anyhow::Result<()> where T: OnNewSubscriptionHandler {
        unsafe {
            if libaeron_sys::aeron_context_set_on_new_subscription(
                self.ptr,
                Some(on_new_subscription_handler_trampoline::<T>),
                &mut handler as *mut _ as *mut std::os::raw::c_void,
            ) < 0
            {
                bail!(format!(
                    "aeron_context_set_on_new_subscription: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            Ok(())
        }
    }

    pub fn set_new_publication_handler<T>(
        &self,
        mut handler: &T,
    ) -> anyhow::Result<()> where T: OnNewPublicationHandler {
        unsafe {
            if libaeron_sys::aeron_context_set_on_new_publication(
                self.ptr,
                Some(on_new_publication_handler_trampoline::<T>),
                &mut handler as *mut _ as *mut std::os::raw::c_void,
            ) < 0
            {
                bail!(format!(
                    "aeron_context_set_on_new_publication: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            Ok(())
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            libaeron_sys::aeron_context_close(self.ptr);
        }
    }
}