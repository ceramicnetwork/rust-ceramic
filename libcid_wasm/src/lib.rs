use libcid;
use multibase::Base;
//use wasm_bindgen::prelude::wasm_bindgen;

//#[wasm_bindgen]
//extern "C" {
//    // Use `js_namespace` here to bind `console.log(..)` instead of just
//    // `log(..)`
//    #[wasm_bindgen(js_namespace = console)]
//    fn log(s: &str);
//}
//macro_rules! console_log {
//    // Note that this is using the `log` function imported above during
//    // `bare_bones`
//    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
//}

//#[wasm_bindgen]
pub fn compute_cid(data: &[u8]) -> String {
    //console_log!("compute_cid: {:?}", data);
    let cid = libcid::compute_cid(data);
    cid.to_string_of_base(Base::Base32Lower).unwrap()
}

//#[wasm_bindgen]
pub fn store(data: &[u8]) -> String {
    //console_log!("store: {:?}", data);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let cid = rt.block_on(libcid::store(data));
    cid.to_string_of_base(Base::Base32Lower).unwrap()
}
