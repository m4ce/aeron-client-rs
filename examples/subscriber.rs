use std::ffi::CStr;
use std::io;
use std::io::Write;
use aeron_client_rs::client::{AeronImage, Client, Context, ErrorHandler, FragmentAssembler, FragmentHandler, OnAvailableImageHandler, OnNewPublication, OnNewSubscription, OnUnavailableImageHandler};

pub struct DefaultOnAvailableImageHandler {
}

impl OnAvailableImageHandler for DefaultOnAvailableImageHandler {
    fn handle(&self, registration_id: i64, image: AeronImage) {
        println!("Image has become available [sessionId={}]", image.session_id());
    }
}

pub struct DefaultOnUnAvailableImageHandler {
}

impl OnUnavailableImageHandler for DefaultOnUnAvailableImageHandler {
    fn handle(&self, registration_id: i64, image: AeronImage) {
        println!("Image has become unavailable [sessionId={}]", image.session_id());
    }
}

pub struct DefaultFragmentHandler {
}

impl FragmentHandler for DefaultFragmentHandler {
    fn on_fragment(&self, data: &[u8], _header: &libaeron_sys::aeron_header_t) {
        println!("Received fragment: {}", data.len());
    }
}

pub struct DefaultErrorHandler {

}

impl ErrorHandler for DefaultErrorHandler {
    fn on_error(&self, code: i32, msg: &CStr) {
        eprintln!("Caught error [code={}, msg={:?}]", code, msg);
    }
}

pub struct DefaultOnNewSubscriptionHandler {
}

impl OnNewSubscription for DefaultOnNewSubscriptionHandler {
    fn handle(&self, channel: &CStr, stream_id: i32, correlation_id: i64) {
        println!("Registered new subscription on channel={:?}, streamId={}, correlationId={}", channel, stream_id, correlation_id);
        io::stdout().flush();
    }
}

fn main() -> anyhow::Result<()> {
    let error_handler = DefaultErrorHandler{};
    let on_new_subscription_handler = DefaultOnNewSubscriptionHandler{};
    let mut context = Context::new()?;
    context.set_dir("/Volumes/DevShm/aeron".into())?;
    context.set_use_conductor_agent_invoker(true)?;
    context.set_error_handler(&error_handler)?;
    context.set_new_subscription_handler(&on_new_subscription_handler)?;
    let mut client = Client::new(&context)?;
    println!("client id: {}", client.client_id());
    let on_available_image_handler = DefaultOnAvailableImageHandler {};
    let on_unavailable_image_handler = DefaultOnUnAvailableImageHandler {};
    let registration_id = client.async_add_subscription("aeron:ipc".into(), 1, &on_available_image_handler, &on_unavailable_image_handler)?;
    let fragment_handler = DefaultFragmentHandler{};
    let assembler = FragmentAssembler::new(&fragment_handler)?;
    // let processor = DefaultFragmentProcessor::new(&fragment_handler);
    loop {
        client.poll()?;
        let subscription = client.find_subscription(registration_id).unwrap();
        if let Ok(value) = subscription.poll_ready() {
            if !value {
                continue;
            }
        }
        subscription.poll(&assembler.processor(), 10)?;
    }
    // sleep(Duration::from_secs(10));
    Ok(())
}