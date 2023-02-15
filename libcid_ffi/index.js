const koffi = require('koffi');


// Load the shared library
const lib = koffi.load('../target/debug/libcid.so');

const free_cid = lib.cdecl('free_cid', 'void', ['void*']);
const heapStr = koffi.disposable('heapStr', 'char*', free_cid);
const compute_cid = lib.cdecl('compute_cid', heapStr, ['uint8*', 'int']);
const store = lib.cdecl('store', heapStr, ['uint8*', 'int']);

const buf = Buffer.from('hello world');
const cid = compute_cid(buf, buf.length);
console.log(cid);
console.log(cid == 'bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e');

const stored_cid = store(buf, buf.length);
console.log(stored_cid);
console.log(stored_cid == 'bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e');


