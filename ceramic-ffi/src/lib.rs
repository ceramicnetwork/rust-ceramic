use std::ffi::{c_char, c_uchar, CStr};

#[repr(C)]
pub struct Bytes {
    data: *const c_uchar,
    len: u32,
}

#[no_mangle]
pub extern "C" fn ceramic_kubo_dag_get(cid: *const c_char) -> Bytes {
    println!("ceramic_kubo_dag_get");
    let cid_cstr = unsafe { CStr::from_ptr(cid) };
    let cid_str = cid_cstr.to_string_lossy();
    let data = cid_str.as_bytes();
    Bytes {
        data: data.as_ptr(),
        len: data.len() as u32,
    }
}
