use std::ffi::CStr;
use std::io;
use std::io::Write;
use std::mem::size_of;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use aeron_client_rs::client::{Client, Context, ErrorHandler, OnNewPublication};

fn nanos_since_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

pub struct DefaultErrorHandler {

}

impl ErrorHandler for DefaultErrorHandler {
    fn on_error(&self, code: i32, msg: &CStr) {
        eprintln!("Caught error [code={}, msg={:?}]", code, msg);
        io::stderr().flush();
    }
}

pub struct DefaultOnNewPublicationHandler {
}

impl OnNewPublication for DefaultOnNewPublicationHandler {
    fn handle(&self, channel: &CStr, stream_id: i32, session_id: i32, correlation_id: i64) {
        println!("Registered new publication on channel={:?}, streamId={}, sessionId={}, correlationId={}", channel, stream_id, session_id, correlation_id);
        io::stdout().flush();
    }
}

fn main() -> anyhow::Result<()> {
    let error_handler = DefaultErrorHandler{};
    let on_new_publication_handler = DefaultOnNewPublicationHandler{};
    let mut context = Context::new()?;
    context.set_dir("/Volumes/DevShm/aeron".into())?;
    context.set_use_conductor_agent_invoker(true)?;
    context.set_error_handler(&error_handler)?;
    context.set_new_publication_handler(&on_new_publication_handler)?;
    let mut client = Client::new(&context)?;
    println!("client id: {}", client.client_id());
    let registration_id = client.async_add_publication("aeron:ipc".into(), 1)?;
    loop {
        client.poll()?;
        let publication = client.find_publication(registration_id).unwrap();
        if let Ok(value) = publication.poll_ready() {
            if !value {
                continue;
            }
        }
        if publication.is_connected() {
            let mut buffer_claim = publication.try_claim(size_of::<i64>())?;
            buffer_claim.as_mut_slice().copy_from_slice(&i64::to_le_bytes(nanos_since_epoch()));
            buffer_claim.commit()?;
            sleep(Duration::from_millis(1000));
        }
    }
    // sleep(Duration::from_secs(10));
    Ok(())
}