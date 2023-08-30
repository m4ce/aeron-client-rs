use anyhow::bail;
use core::slice;
use std::collections::HashMap;
use std::ffi::{CStr};
use std::marker::PhantomData;
use std::ptr::{null, null_mut};

pub trait ErrorHandler {
    fn on_error(&self, code: i32, msg: &CStr);
}

pub trait FragmentHandler {
    fn on_fragment(&self, _data: &[u8], _header: &libaeron_sys::aeron_header_t);
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
        Some(fragment_handler::<T>)
    }

    fn user_data(&self) -> *mut std::os::raw::c_void {
        self.handler_ptr
    }
}

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

unsafe extern "C" fn on_unavailable_image_handler<T: OnUnavailableImageHandler>(clientd: *mut std::os::raw::c_void, registration_id: i64, subscription: *mut libaeron_sys::aeron_subscription_t, image: *mut libaeron_sys::aeron_image_t) {
    // trampoline
    let handler = clientd as *mut T;
    (*handler).handle(registration_id, AeronImage::wrap(image));
}

unsafe extern "C" fn on_available_image_handler<T: OnAvailableImageHandler>(clientd: *mut std::os::raw::c_void, registration_id: i64, subscription: *mut libaeron_sys::aeron_subscription_t, image: *mut libaeron_sys::aeron_image_t) {
    // trampoline
    let handler = clientd as *mut T;
    (*handler).handle(registration_id, AeronImage::wrap(image));
}

unsafe extern "C" fn error_handler<T: ErrorHandler>(clientd: *mut ::std::os::raw::c_void, errcode: std::os::raw::c_int, message: *const ::std::os::raw::c_char) {
    // trampoline
    let handler = clientd as *mut T;
    (*handler).on_error(errcode, CStr::from_ptr(message));
}

unsafe extern "C" fn on_new_subscription_handler<T: OnNewSubscription>(clientd: *mut std::os::raw::c_void, async_: *mut libaeron_sys::aeron_async_add_subscription_t, channel: *const std::os::raw::c_char, stream_id: i32, correlation_id: i64) {
    // trampoline
    let handler = clientd as *mut T;
    (*handler).handle(CStr::from_ptr(channel), stream_id, correlation_id);
}

unsafe extern "C" fn on_new_publication_handler<T: OnNewPublication>(clientd: *mut ::std::os::raw::c_void, async_: *mut libaeron_sys::aeron_async_add_publication_t, channel: *const ::std::os::raw::c_char, stream_id: i32, session_id: i32, correlation_id: i64) {
    // trampoline
    let handler = clientd as *mut T;
    (*handler).handle(CStr::from_ptr(channel), stream_id, session_id, correlation_id);
}

unsafe extern "C" fn fragment_handler<T: FragmentHandler>(
    clientd: *mut std::os::raw::c_void,
    buffer: *const u8,
    length: usize,
    header: *mut libaeron_sys::aeron_header_t,
) {
    // trampoline
    let handler = clientd as *mut T;
    (*handler).on_fragment(slice::from_raw_parts(buffer, length), &*header);
}

unsafe extern "C" fn fragment_assembler_handler(clientd: *mut std::os::raw::c_void, buffer: *const u8, length: usize, header: *mut libaeron_sys::aeron_header_t) {
    libaeron_sys::aeron_fragment_assembler_handler(clientd, buffer, length, header);
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
                Some(fragment_handler::<T>),
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
            inner: Some(fragment_assembler_handler)
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

pub trait OnNewSubscription {
    fn handle(&self, channel: &CStr, stream_id: i32, correlation_id: i64);
}

pub trait OnNewPublication {
    fn handle(&self, channel: &CStr, stream_id: i32, session_id: i32, correlation_id: i64);
}

pub trait OnAvailableImageHandler {
    fn handle(&self, registration_id: i64, image: AeronImage);
}

pub trait OnUnavailableImageHandler {
    fn handle(&self, registration_id: i64, image: AeronImage);
}

pub struct AeronImage {
    handle: *mut libaeron_sys::aeron_image_t,
}

impl AeronImage {
    fn wrap(handle: *mut libaeron_sys::aeron_image_t) -> Self {
        Self { handle }
    }

    pub fn session_id(&self) -> i32 {
        unsafe {
            libaeron_sys::aeron_image_session_id(self.handle)
        }
    }

    pub fn is_eof(&self) -> bool {
        unsafe {
            libaeron_sys::aeron_image_is_end_of_stream(self.handle)
        }
    }

    pub fn eof_position(&self) -> i64 {
        unsafe {
            libaeron_sys::aeron_image_end_of_stream_position(self.handle)
        }
    }

    pub fn is_closed(&self) -> bool {
        unsafe {
            libaeron_sys::aeron_image_is_closed(self.handle)
        }
    }
}

pub struct BufferClaim {
    handle: libaeron_sys::aeron_buffer_claim_t,
    committed: bool,
    aborted: bool,
}

impl BufferClaim {
    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.handle.data, self.handle.length)
        }
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.verify_claim_not_released()?;
        unsafe {
            if libaeron_sys::aeron_buffer_claim_commit(&mut self.handle) < 0 {
                bail!(format!(
                    "aeron_buffer_claim_commit: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            self.committed = true;
            Ok(())
        }
    }

    pub fn abort(&mut self) -> anyhow::Result<()> {
        self.verify_claim_not_released()?;
        unsafe {
            if libaeron_sys::aeron_buffer_claim_abort(&mut self.handle) < 0 {
                bail!(format!(
                    "aeron_buffer_claim_abort: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            Ok(())
        }
    }

    fn verify_claim_not_released(&self) -> anyhow::Result<()> {
        if self.committed {
            bail!("claim space committed");
        }
        if self.aborted {
            bail!("claim space aborted");
        }
        Ok(())
    }
}

impl Drop for BufferClaim {
    fn drop(&mut self) {
        if !self.committed {
            self.abort().expect("failed to abort claim");
        }
    }
}

pub struct Context {
    context: *mut libaeron_sys::aeron_context_t,
    directory: String
}

impl Context {
    #[cfg(target_os = "linux")]
    pub const DEFAULT_AERON_DIRECTORY: &'static str = "/dev/shm/aeron";
    #[cfg(target_os = "macos")]
    pub const DEFAULT_AERON_DIRECTORY: &'static str = "/Volumes/DevShm/aeron";

    pub fn new() -> anyhow::Result<Self> {
        let mut context = Self {
            context: null_mut(),
            directory: "".into()
        };
        unsafe {
            if libaeron_sys::aeron_context_init(&mut context.context) < 0 {
                bail!(format!(
                    "aeron_context_init: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        context.set_dir(Context::DEFAULT_AERON_DIRECTORY.into());
        Ok(context)
    }

    pub fn set_use_conductor_agent_invoker(&mut self, value: bool) -> anyhow::Result<()> {
        unsafe {
            if libaeron_sys::aeron_context_set_use_conductor_agent_invoker(self.context, value) < 0 {
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
            if libaeron_sys::aeron_context_set_dir(self.context, self.directory.as_ptr() as *const std::os::raw::c_char) < 0
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
                self.context,
                Some(error_handler::<T>),
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
    ) -> anyhow::Result<()> where T: OnNewSubscription {
        unsafe {
            if libaeron_sys::aeron_context_set_on_new_subscription(
                self.context,
                Some(on_new_subscription_handler::<T>),
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
    ) -> anyhow::Result<()> where T: OnNewPublication {
        unsafe {
            if libaeron_sys::aeron_context_set_on_new_publication(
                self.context,
                Some(on_new_publication_handler::<T>),
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
            libaeron_sys::aeron_context_close(self.context);
        }
    }
}

pub struct Client<'a> {
    handle: *mut libaeron_sys::aeron_t,
    context: &'a Context,
    subscriptions: HashMap<i64, Subscription>,
    publications: HashMap<i64, Publication>,
}

impl Drop for Client<'_> {
    fn drop(&mut self) {
        // release resources
        self.subscriptions.clear();
        self.publications.clear();
        unsafe {
            libaeron_sys::aeron_close(self.handle);
        }
    }
}

pub struct Subscription {
    channel: String,
    async_: *mut libaeron_sys::aeron_async_add_subscription_t,
    handle: *mut libaeron_sys::aeron_subscription_t,
}

impl Subscription {
    pub fn poll_ready(&mut self) -> anyhow::Result<bool> {
        if !self.handle.is_null() {
            return Ok(true);
        }
        unsafe {
            match libaeron_sys::aeron_async_add_subscription_poll(&mut self.handle, self.async_) {
                0 => Ok(false),
                1 => Ok(!self.handle.is_null()),
                _ => bail!(format!(
                    "aeron_async_add_subscription_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }

    pub fn channel_status(&self) -> i64 {
        unsafe { libaeron_sys::aeron_subscription_channel_status(self.handle) }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { libaeron_sys::aeron_subscription_is_connected(self.handle) }
    }

    // pub fn async_add_destination(
    //     &self,
    //     endpoint_channel: CString,
    //     client: &Client,
    // ) -> anyhow::Result<Destination> {
    //     let mut async_destination: Destination = Destination { async_: null_mut() };
    //     unsafe {
    //         if libaeron_sys::aeron_subscription_async_add_destination(
    //             &mut async_destination.async_,
    //             client.handle,
    //             self.handle,
    //             endpoint_channel.as_ptr(),
    //         ) < 0
    //         {
    //             bail!(format!(
    //                 "aeron_subscription_async_add_destination: {:?}",
    //                 CStr::from_ptr(libaeron_sys::aeron_errmsg())
    //             ));
    //         }
    //     }
    //     Ok(async_destination)
    // }
    //
    // pub fn async_remove_destination(
    //     &self,
    //     endpoint_channel: CString,
    //     client: &Client,
    // ) -> anyhow::Result<Destination> {
    //     let mut async_destination: Destination = Destination { async_: null_mut() };
    //     unsafe {
    //         if libaeron_sys::aeron_subscription_async_remove_destination(
    //             &mut async_destination.async_,
    //             client.handle,
    //             self.handle,
    //             endpoint_channel.as_ptr(),
    //         ) < 0
    //         {
    //             bail!(format!(
    //                 "aeron_subscription_async_remove_destination: {:?}",
    //                 CStr::from_ptr(libaeron_sys::aeron_errmsg())
    //             ));
    //         }
    //     }
    //     Ok(async_destination)
    // }

    pub fn image_at_index(&self, index: usize) -> anyhow::Result<AeronImage> {
        unsafe {
            let x = libaeron_sys::aeron_subscription_image_at_index(self.handle, index);
            if x.is_null() {
                bail!(format!("No image exists at index {}", index));
            }
            Ok(AeronImage::wrap(x))
        }
    }

    pub fn image_count(&self) -> i32 {
        unsafe { libaeron_sys::aeron_subscription_image_count(self.handle) }
    }

    pub fn poll<T>(&self, fragment_processor: &T, fragment_limit: usize) -> anyhow::Result<i32>
    where
        T: FragmentProcessor,
    {
        unsafe {
            match libaeron_sys::aeron_subscription_poll(
                self.handle,
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
        unsafe { libaeron_sys::aeron_subscription_is_closed(self.handle) }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        unsafe {
            libaeron_sys::aeron_subscription_close(self.handle, None, null_mut());
        }
    }
}

trait DestinationReadiness {
    fn ready(async_: *mut libaeron_sys::aeron_async_destination_t) -> anyhow::Result<bool>;
}

struct PublicationAsyncDestination {}

impl DestinationReadiness for PublicationAsyncDestination {
    fn ready(async_: *mut libaeron_sys::aeron_async_destination_t) -> anyhow::Result<bool> {
        unsafe {
            match libaeron_sys::aeron_publication_async_destination_poll(async_) {
                0 => Ok(false),
                1 => Ok(true),
                _ => bail!(format!(
                    "aeron_publication_async_destination_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }
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

pub struct Destination {
    async_: *mut libaeron_sys::aeron_async_destination_t,
}

impl Destination {
    fn ready<T>(&self) -> anyhow::Result<bool>
    where
        T: DestinationReadiness,
    {
        T::ready(self.async_)
    }
}

pub struct Publication {
    channel: String,
    async_: *mut libaeron_sys::aeron_async_add_publication_t,
    handle: *mut libaeron_sys::aeron_publication_t,
}

impl Publication {
    pub fn poll_ready(&mut self) -> anyhow::Result<bool> {
        if !self.handle.is_null() {
            return Ok(true);
        }
        unsafe {
            match libaeron_sys::aeron_async_add_publication_poll(&mut self.handle, self.async_) {
                0 => Ok(false),
                1 => Ok(!self.handle.is_null()),
                _ => bail!(format!(
                    "aeron_async_add_publication_poll: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
            }
        }
    }

    pub fn channel_status(&self) -> i64 {
        unsafe { libaeron_sys::aeron_publication_channel_status(self.handle) }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { libaeron_sys::aeron_publication_is_connected(self.handle) }
    }

    pub fn channel(&self) -> &str {
        self.channel.as_str()
    }

    pub fn stream_id(&self) -> i32 {
        unsafe { libaeron_sys::aeron_publication_stream_id(self.handle) }
    }

    pub fn session_id(&self) -> i32 {
        unsafe { libaeron_sys::aeron_publication_session_id(self.handle) }
    }

    pub fn offer(&self, data: &[u8]) -> anyhow::Result<()> {
        unsafe {
            if libaeron_sys::aeron_publication_offer(
                self.handle,
                data.as_ptr(),
                data.len(),
                None,
                null_mut(),
            ) < 0
            {
                bail!(format!(
                    "aeron_publication_offer: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(())
    }

    pub fn try_claim(&self, length: usize) -> anyhow::Result<BufferClaim> {
        let mut claim = BufferClaim {
            handle: libaeron_sys::aeron_buffer_claim_t{
                frame_header: null_mut(),
                data: null_mut(),
                length: 0,
            },
            committed: false,
            aborted: false,
        };
        unsafe {
            if libaeron_sys::aeron_publication_try_claim(self.handle, length, &mut claim.handle) < 0 {
                bail!(format!(
                    "aeron_publication_try_claim: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(claim)
    }

    // pub fn async_add_destination(
    //     &self,
    //     endpoint_channel: CString,
    //     client: &Client,
    // ) -> anyhow::Result<Destination> {
    //     let mut async_destination: Destination = Destination { async_: null_mut() };
    //     unsafe {
    //         if libaeron_sys::aeron_publication_async_add_destination(
    //             &mut async_destination.async_,
    //             client.handle,
    //             self.handle,
    //             endpoint_channel.as_ptr(),
    //         ) < 0
    //         {
    //             bail!(format!(
    //                 "aeron_publication_async_add_destination: {:?}",
    //                 CStr::from_ptr(libaeron_sys::aeron_errmsg())
    //             ));
    //         }
    //     }
    //     Ok(async_destination)
    // }
    //
    // pub fn async_remove_destination(
    //     &self,
    //     endpoint_channel: CString,
    //     client: &Client,
    // ) -> anyhow::Result<Destination> {
    //     let mut async_destination: Destination = Destination { async_: null_mut() };
    //     unsafe {
    //         if libaeron_sys::aeron_publication_async_remove_destination(
    //             &mut async_destination.async_,
    //             client.handle,
    //             self.handle,
    //             endpoint_channel.as_ptr(),
    //         ) < 0
    //         {
    //             bail!(format!(
    //                 "aeron_publication_async_remove_destination: {:?}",
    //                 CStr::from_ptr(libaeron_sys::aeron_errmsg())
    //             ));
    //         }
    //     }
    //     Ok(async_destination)
    // }

    pub fn is_closed(&self) -> bool {
        unsafe { libaeron_sys::aeron_publication_is_closed(self.handle) }
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        unsafe {
            libaeron_sys::aeron_publication_close(self.handle, None, null_mut());
        }
    }
}

impl<'a> Client<'a> {
    pub fn new(context: &'a Context) -> anyhow::Result<Self> {
        let mut client = Self {
            handle: null_mut(),
            context,
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
        };
        unsafe {
            if libaeron_sys::aeron_init(&mut client.handle, context.context) < 0 {
                bail!(format!(
                    "aeron_init: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }

            if libaeron_sys::aeron_start(client.handle) < 0 {
                bail!(format!(
                    "aeron_start: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
        }
        Ok(client)
    }

    pub fn client_id(&self) -> i64 {
        unsafe { libaeron_sys::aeron_client_id(self.handle) }
    }

    pub fn poll(&self) -> anyhow::Result<i32> {
        unsafe {
            match libaeron_sys::aeron_main_do_work(self.handle) {
                -1 => bail!(format!(
                    "aeron_main_do_work: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                )),
                work => Ok(work)
            }
        }
    }

    pub fn async_add_publication(&mut self, channel: String, stream_id: i32) -> anyhow::Result<i64> {
        let mut async_publication = Publication {
            channel,
            async_: null_mut(),
            handle: null_mut(),
        };
        let registration_id: i64;
        unsafe {
            if libaeron_sys::aeron_async_add_publication(
                &mut async_publication.async_,
                self.handle,
                async_publication.channel.as_ptr() as *const std::os::raw::c_char,
                stream_id,
            ) < 0
            {
                bail!(format!(
                    "aeron_async_add_publication: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            registration_id = libaeron_sys::aeron_async_add_publication_get_registration_id(
                async_publication.async_,
            );
            self.publications.insert(registration_id, async_publication);
        }
        Ok(registration_id)
    }

    pub fn find_publication(&mut self, registration_id: i64) -> Option<&mut Publication> {
        self.publications.get_mut(&registration_id)
    }

    pub fn find_subscription(&mut self, registration_id: i64) -> Option<&mut Subscription> {
        self.subscriptions.get_mut(&registration_id)
    }

    pub fn close_publication(&mut self, registration_id: i64) {
        // drop should be invoked
        self.publications.remove(&registration_id);
    }

    pub fn close_subscription(&mut self, registration_id: i64) {
        // drop should be invoked
        self.subscriptions.remove(&registration_id);
    }

    pub fn add_publication(&mut self, channel: String, stream_id: i32) -> anyhow::Result<i64> {
        let registration_id = self.async_add_publication(channel, stream_id)?;
        let mut publication = self.find_publication(registration_id).unwrap();
        loop {
            match publication.poll_ready() {
                Ok(b) => {
                    if b {
                        return Ok(registration_id);
                    }
                    // keep waiting ...
                },
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
    ) -> anyhow::Result<i64> where A: OnAvailableImageHandler, U: OnUnavailableImageHandler {
        let mut async_subscription = Subscription {
            channel,
            async_: null_mut(),
            handle: null_mut(),
        };
        unsafe {
            if libaeron_sys::aeron_async_add_subscription(
                &mut async_subscription.async_,
                self.handle,
                async_subscription.channel.as_ptr() as *const std::os::raw::c_char,
                stream_id,
                Some(on_available_image_handler::<A>),
                &mut available_image_handler as *mut _ as *mut std::os::raw::c_void,
                Some(on_unavailable_image_handler::<U>),
                &mut unavailable_image_handler as *mut _ as *mut std::os::raw::c_void
            ) < 0
            {
                bail!(format!(
                    "aeron_async_add_subscription: {:?}",
                    CStr::from_ptr(libaeron_sys::aeron_errmsg())
                ));
            }
            let registration_id = libaeron_sys::aeron_async_add_subscription_get_registration_id(
                async_subscription.async_,
            );
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
        let mut subscription = self.find_subscription(registration_id).unwrap();
        loop {
            match subscription.poll_ready() {
                Ok(b) => {
                    if b {
                        return Ok(registration_id);
                    }
                    // keep waiting ...
                },
                Err(e) => {
                    bail!(e)
                }
            }
        }
    }
}
