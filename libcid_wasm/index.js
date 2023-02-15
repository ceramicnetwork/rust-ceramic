const libcid = require('./pkg/libcid_wasm.js');

const cid = libcid.compute_cid(Buffer.from('hello world'));
console.log(cid == 'bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e');
