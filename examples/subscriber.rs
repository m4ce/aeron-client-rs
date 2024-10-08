use aeron_client_rs::client::{Client, OnAvailableImageHandler, OnUnavailableImageHandler};
use aeron_client_rs::context::{Context, ErrorHandler, OnNewSubscriptionHandler};
use aeron_client_rs::fragment_assembler::FragmentAssembler;
use aeron_client_rs::fragment_processor::FragmentHandler;
use aeron_client_rs::image::Image;
use std::ffi::CStr;
use aeron_client_rs::header::Header;

pub struct DefaultOnAvailableImageHandler {}

impl OnAvailableImageHandler for DefaultOnAvailableImageHandler {
    fn handle(&self, registration_id: i64, image: &Image) {
        println!(
            "Image has become available [sessionId={}]",
            image.session_id()
        );
    }
}

pub struct DefaultOnUnAvailableImageHandler {}

impl OnUnavailableImageHandler for DefaultOnUnAvailableImageHandler {
    fn handle(&self, registration_id: i64, image: &Image) {
        println!(
            "Image has become unavailable [sessionId={}]",
            image.session_id()
        );
    }
}

pub struct DefaultFragmentHandler {}

impl FragmentHandler for DefaultFragmentHandler {
    fn on_fragment(&mut self, data: &[u8], header: &Header) {
        println!(
            "Received fragment: [value={}, len={}, sessionId={}, streamId={}, reservedValue={}]",
            i64::from_le_bytes(data[0..8].try_into().unwrap()),
            data.len(),
            header.session_id(),
            header.stream_id(),
            header.reserved_value()
        );
    }
}

pub struct DefaultErrorHandler {}

impl ErrorHandler for DefaultErrorHandler {
    fn on_error(&self, code: i32, msg: &CStr) {
        eprintln!("Caught error [code={}, msg={:?}]", code, msg);
    }
}

pub struct DefaultOnNewSubscriptionHandler {}

impl OnNewSubscriptionHandler for DefaultOnNewSubscriptionHandler {
    fn handle(&self, channel: &CStr, stream_id: i32, correlation_id: i64) {
        println!(
            "Registered new subscription on channel={:?}, streamId={}, correlationId={}",
            channel, stream_id, correlation_id
        );
    }
}

fn main() -> anyhow::Result<()> {
    let error_handler = DefaultErrorHandler {};
    let on_new_subscription_handler = DefaultOnNewSubscriptionHandler {};
    let mut context = Context::new()?;
    context.set_dir("/Volumes/DevShm/aeron".into())?;
    context.set_use_conductor_agent_invoker(true)?;
    context.set_error_handler(&error_handler)?;
    context.set_new_subscription_handler(&on_new_subscription_handler)?;
    let mut client = Client::new(&context)?;
    println!("client id: {}", client.client_id());
    let on_available_image_handler = DefaultOnAvailableImageHandler {};
    let on_unavailable_image_handler = DefaultOnUnAvailableImageHandler {};
    let registration_id = client.async_add_subscription(
        "aeron:ipc".into(),
        1,
        &on_available_image_handler,
        &on_unavailable_image_handler,
    )?;
    println!("registration id: {}", registration_id);
    let fragment_handler = DefaultFragmentHandler {};
    let assembler = FragmentAssembler::new(&fragment_handler)?;
    // let processor = DefaultFragmentProcessor::new(&fragment_handler);
    loop {
        client.poll()?;
        match client.find_subscription(registration_id)? {
            Some(subscription) => {
                subscription.poll(&assembler.processor(), 10)?
            },
            None => continue
        };
    }
    // sleep(Duration::from_secs(10));
    Ok(())
}
