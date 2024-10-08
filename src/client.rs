use crate::context::Context;
use crate::exclusive_publication::ExclusivePublication;
use crate::image::Image;
use crate::publication::Publication;
use crate::subscription::Subscription;
use anyhow::bail;
use std::collections::HashMap;
use std::ffi::CStr;
use std::ptr::null_mut;

unsafe extern "C" fn on_unavailable_image_handler_trampoline<T: OnUnavailableImageHandler>(
    clientd: *mut std::os::raw::c_void,
    registration_id: i64,
    subscription: *mut libaeron_sys::aeron_subscription_t,
    image: *mut libaeron_sys::aeron_image_t,
) {
    let handler = clientd as *mut T;
    let img = Image::new(image, null_mut());
    (*handler).handle(registration_id, &img);
}

unsafe extern "C" fn on_available_image_handler_trampoline<T: OnAvailableImageHandler>(
    clientd: *mut std::os::raw::c_void,
    registration_id: i64,
    subscription: *mut libaeron_sys::aeron_subscription_t,
    image: *mut libaeron_sys::aeron_image_t,
) {
    let handler = clientd as *mut T;
    let img = Image::new(image, null_mut());
    (*handler).handle(registration_id, &img);
}

pub trait OnAvailableImageHandler {
    fn handle(&self, registration_id: i64, image: &Image);
}

pub trait OnUnavailableImageHandler {
    fn handle(&self, registration_id: i64, image: &Image);
}

pub struct Client<'a> {
    ptr: *mut libaeron_sys::aeron_t,
    context: &'a Context,
    subscriptions: HashMap<i64, Subscription>,
    publications: HashMap<i64, Publication>,
    exclusive_publications: HashMap<i64, ExclusivePublication>,
}

impl Drop for Client<'_> {
    fn drop(&mut self) {
        // release resources
        self.subscriptions.clear();
        self.publications.clear();
        self.exclusive_publications.clear();
        unsafe {
            libaeron_sys::aeron_close(self.ptr);
        }
    }
}

impl<'a> Client<'a> {
    pub fn new(context: &'a Context) -> anyhow::Result<Self> {
        let mut client = Self {
            ptr: null_mut(),
            context,
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
            exclusive_publications: HashMap::new(),
        };
        unsafe {
            if libaeron_sys::aeron_init(&mut client.ptr, context.ptr()) < 0 {
                bail!(format!(
                    "aeron_init: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }

            if libaeron_sys::aeron_start(client.ptr) < 0 {
                bail!(format!(
                    "aeron_start: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(client)
    }

    pub fn client_id(&self) -> i64 {
        unsafe { libaeron_sys::aeron_client_id(self.ptr) }
    }

    pub fn next_correlation_id(&self) -> i64 {
        unsafe { libaeron_sys::aeron_next_correlation_id(self.ptr) }
    }

    pub fn poll(&self) -> anyhow::Result<i32> {
        unsafe {
            match libaeron_sys::aeron_main_do_work(self.ptr) {
                -1 => bail!(format!(
                    "aeron_main_do_work: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
                work => Ok(work),
            }
        }
    }

    pub fn find_publication(&mut self, registration_id: i64) -> anyhow::Result<Option<&Publication>> {
        if let Some(publication) = self.publications.get_mut(&registration_id) {
            if publication.is_ready() {
                return Ok(Some(publication));
            }
            unsafe {
                match libaeron_sys::aeron_async_add_publication_poll(publication.mut_ptr(), publication.async_ptr()) {
                    0 => Ok(None),
                    1 => {
                        if publication.is_ready() {
                            Ok(Some(publication))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => bail!(format!(
                    "aeron_async_add_publication_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn find_exclusive_publication(
        &mut self,
        registration_id: i64,
    ) -> anyhow::Result<Option<&ExclusivePublication>> {
        if let Some(exclusive_publication) = self.exclusive_publications.get_mut(&registration_id) {
            if exclusive_publication.is_ready() {
                return Ok(Some(exclusive_publication));
            }
            unsafe {
                match libaeron_sys::aeron_async_add_exclusive_publication_poll(exclusive_publication.mut_ptr(), exclusive_publication.async_ptr()) {
                    0 => Ok(None),
                    1 => {
                        if exclusive_publication.is_ready() {
                            Ok(Some(exclusive_publication))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => bail!(format!(
                    "aeron_async_add_exclusive_publication_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn find_subscription(&mut self, registration_id: i64) -> anyhow::Result<Option<&Subscription>> {
        if let Some(subscription) = self.subscriptions.get_mut(&registration_id) {
            if subscription.is_ready() {
                return Ok(Some(subscription));
            }
            unsafe {
                match libaeron_sys::aeron_async_add_subscription_poll(subscription.mut_ptr(), subscription.async_ptr()) {
                    0 => Ok(None),
                    1 => {
                        if subscription.is_ready() {
                            Ok(Some(subscription))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => bail!(format!(
                    "aeron_async_add_subscription_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn close_publication(&mut self, registration_id: i64) {
        // drop should be invoked
        self.publications.remove(&registration_id);
    }

    pub fn close_exclusive_publication(&mut self, registration_id: i64) {
        // drop should be invoked
        self.exclusive_publications.remove(&registration_id);
    }

    pub fn close_subscription(&mut self, registration_id: i64) {
        // drop should be invoked
        self.subscriptions.remove(&registration_id);
    }

    pub fn async_add_publication(
        &mut self,
        channel: String,
        stream_id: i32,
    ) -> anyhow::Result<i64> {
        let mut async_publication = Publication::new(channel, self.ptr);
        let registration_id: i64;
        unsafe {
            if libaeron_sys::aeron_async_add_publication(
                async_publication.async_mut_ptr(),
                self.ptr,
                async_publication.channel().as_ptr() as *const std::os::raw::c_char,
                stream_id,
            ) < 0
            {
                bail!(format!(
                    "aeron_async_add_publication: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            assert!(!async_publication.async_ptr().is_null());
            registration_id = (*async_publication.async_ptr()).registration_id;
            self.publications.insert(registration_id, async_publication);
        }
        Ok(registration_id)
    }

    pub fn add_publication(&mut self, channel: String, stream_id: i32) -> anyhow::Result<i64> {
        let registration_id = self.async_add_publication(channel, stream_id)?;
        loop {
            match self.find_publication(registration_id) {
                Ok(Some(_)) => {
                    return Ok(registration_id);
                }
                Ok(None) => {
                    // keep waiting ...
                }
                Err(e) => {
                    bail!(e)
                }
            }
        }
    }

    pub fn async_add_exclusive_publication(
        &mut self,
        channel: String,
        stream_id: i32,
    ) -> anyhow::Result<i64> {
        let mut async_exclusive_publication = ExclusivePublication::new(channel, self.ptr);
        let registration_id: i64;
        unsafe {
            if libaeron_sys::aeron_async_add_exclusive_publication(
                async_exclusive_publication.async_mut_ptr(),
                self.ptr,
                async_exclusive_publication.channel().as_ptr() as *const std::os::raw::c_char,
                stream_id,
            ) < 0
            {
                bail!(format!(
                    "aeron_async_add_publication: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            assert!(!async_exclusive_publication.async_ptr().is_null());
            registration_id = (*async_exclusive_publication.async_ptr()).registration_id;
            self.exclusive_publications
                .insert(registration_id, async_exclusive_publication);
        }
        Ok(registration_id)
    }

    pub fn add_exclusive_publication(
        &mut self,
        channel: String,
        stream_id: i32,
    ) -> anyhow::Result<i64> {
        let registration_id = self.async_add_exclusive_publication(channel, stream_id)?;
        loop {
            match self.find_exclusive_publication(registration_id) {
                Ok(Some(_)) => {
                    return Ok(registration_id);
                }
                Ok(None) => {
                    // keep waiting ...
                }
                Err(e) => {
                    bail!(e)
                }
            }
        }
    }

    pub fn async_add_subscription<A, U>(
        &mut self,
        channel: String,
        stream_id: i32,
        mut available_image_handler: &A,
        mut unavailable_image_handler: &U,
    ) -> anyhow::Result<i64>
    where
        A: OnAvailableImageHandler,
        U: OnUnavailableImageHandler,
    {
        let mut async_subscription = Subscription::new(channel, self.ptr);
        unsafe {
            if libaeron_sys::aeron_async_add_subscription(
                async_subscription.async_mut_ptr(),
                self.ptr,
                async_subscription.channel().as_ptr() as *const std::os::raw::c_char,
                stream_id,
                Some(on_available_image_handler_trampoline::<A>),
                &mut available_image_handler as *mut _ as *mut std::os::raw::c_void,
                Some(on_unavailable_image_handler_trampoline::<U>),
                &mut unavailable_image_handler as *mut _ as *mut std::os::raw::c_void,
            ) < 0
            {
                bail!(format!(
                    "aeron_async_add_subscription: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            assert!(!async_subscription.async_ptr().is_null());
            let registration_id = (*async_subscription.async_ptr()).registration_id;
            self.subscriptions
                .insert(registration_id, async_subscription);
            Ok(registration_id)
        }
    }

    pub fn add_subscription<A: OnAvailableImageHandler, U: OnUnavailableImageHandler>(
        &mut self,
        channel: String,
        stream_id: i32,
        available_image_handler: &A,
        unavailable_image_handler: &U,
    ) -> anyhow::Result<i64> {
        let registration_id = self.async_add_subscription::<A, U>(
            channel,
            stream_id,
            available_image_handler,
            unavailable_image_handler,
        )?;

        loop {
            match self.find_subscription(registration_id) {
                Ok(Some(_)) => {
                    return Ok(registration_id);
                }
                Ok(None) => {
                    // keep waiting ...
                }
                Err(e) => {
                    bail!(e)
                }
            }
        }
    }
}
