use core::slice;
use std::ffi::{c_char, c_uchar, CString};

use multibase::Base;

/// Compute the CID of a byte sequence.
/// Returns the base32 lower encoded string of the CID with a RAW codec and SHA2_256 hash.
#[no_mangle]
pub extern "C" fn compute_cid(data: *const c_uchar, len: usize) -> *const c_char {
    println!("compute_cid {:?} {}", data, len);
    let bytes: &[u8] = unsafe { slice::from_raw_parts(data, len) };
    println!("compute_cid {:?} ", bytes);
    let cid = libcid::compute_cid(bytes);
    let ptr = CString::new(cid.to_string_of_base(Base::Base32Lower).unwrap())
        .unwrap()
        .into_raw();

    println!("compute {:?}", ptr);
    ptr
}

#[no_mangle]
pub extern "C" fn store(data: *const c_uchar, len: usize) -> *const c_char {
    println!("store {:?} {}", data, len);
    let bytes: &[u8] = unsafe { slice::from_raw_parts(data, len) };
    println!("store {:?} ", bytes);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let cid = rt.block_on(libcid::store(bytes));
    let ptr = CString::new(cid.to_string_of_base(Base::Base32Lower).unwrap())
        .unwrap()
        .into_raw();

    println!("compute {:?}", ptr);
    ptr
}

/// Free memory associated with cid
#[no_mangle]
pub extern "C" fn free_cid(v: *mut c_char) {
    println!("free {:?}", v);
    unsafe { CString::from_raw(v) };
}
