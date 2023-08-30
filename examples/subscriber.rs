use std::ffi::CStr;
use std::io;
use std::io::Write;
use std::os::raw::c_void;
use libaeron_sys::aeron_fragment_handler_t;
use aeron_client_rs::client::{AeronImage, Client, Context, DefaultFragmentProcessor, ErrorHandler, FragmentAssembler, FragmentHandler, OnAvailableImage, OnUnAvailableImage};

pub struct DefaultOnAvailableImage {
}

impl OnAvailableImage for DefaultOnAvailableImage {
    fn handle(&self, registration_id: i64, image: AeronImage) {
        todo!()
    }
}

pub struct DefaultOnUnAvailableImage {
}

impl OnUnAvailableImage for DefaultOnUnAvailableImage {
    fn handle(&self, registration_id: i64, image: AeronImage) {
        todo!()
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
        io::stderr().flush();
    }
}

fn main() -> anyhow::Result<()> {
    let mut context = Context::new()?;
    context.set_dir("/Volumes/DevShm/aeron".into())?;
    context.set_use_conductor_agent_invoker(true)?;
    context.set_error_handler(Box::new(DefaultErrorHandler{}))?;
    let mut client = Client::new(&context)?;
    println!("client id: {}", client.client_id());
    let on_available_image_handler = DefaultOnAvailableImage{};
    let on_unavailable_image_handler = DefaultOnUnAvailableImage{};
    let registration_id = client.async_add_subscription("aeron:ipc".into(), 1, on_available_image_handler, on_unavailable_image_handler)?;
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