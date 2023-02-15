'use strict';
const { readFile } = require('node:fs/promises');
const { WASI } = require('wasi');
const { argv, env } = require('node:process');
const { join } = require('node:path');

const wasi = new WASI({
  args: argv,
  env,
});

// Some WASI binaries require:
//   const importObject = { wasi_unstable: wasi.wasiImport };
const importObject = { wasi_snapshot_preview1: wasi.wasiImport };

(async () => {
  const wasm = await WebAssembly.compile(
    await readFile(join(__dirname, '../target/wasm32-wasi/debug/libcid_wasm.wasm'))
    //await readFile(join(__dirname, 'demo.wasm'))
  );
  const instance = await WebAssembly.instantiate(wasm, importObject);

  wasi.start(instance);
})();
