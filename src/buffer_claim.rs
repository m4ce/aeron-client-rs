use std::ptr::null_mut;
use std::ffi::{CStr};
use std::slice;
use anyhow::bail;

pub struct BufferClaim {
    claim: libaeron_sys::aeron_buffer_claim_t,
    committed: bool,
    aborted: bool,
}

impl BufferClaim {
    pub(super) fn new() -> Self {
        Self {
            claim: libaeron_sys::aeron_buffer_claim_t{
                frame_header: null_mut(),
                data: null_mut(),
                length: 0,
            },
            committed: false,
            aborted: false
        }
    }

    pub fn is_committed(&self) -> bool {
        self.committed
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted
    }

    pub(super) fn claim(&mut self) -> &mut libaeron_sys::aeron_buffer_claim_t {
        &mut self.claim
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.claim.data, self.claim.length)
        }
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.verify_claim_not_released()?;
        unsafe {
            if libaeron_sys::aeron_buffer_claim_commit(&mut self.claim) < 0 {
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
            if libaeron_sys::aeron_buffer_claim_abort(&mut self.claim) < 0 {
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
        if !self.committed && !self.aborted {
            if !self.claim.data.is_null() {
                unsafe {
                    if libaeron_sys::aeron_buffer_claim_abort(&mut self.claim) < 0 {
                        eprintln!("aeron_buffer_claim_abort: {:?}", CStr::from_ptr(libaeron_sys::aeron_errmsg()));
                    }
                }
            }
        }
    }
}