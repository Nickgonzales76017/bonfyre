import { isAbsolute } from 'node:path';
import { serveOwnedWorker } from './owned_worker_pool.mjs';
import { createV6Bfp } from './v6_bfp.mjs';

serveOwnedWorker(async (payload, { signal }) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload) || payload.operation !== 'verify' ||
      Object.keys(payload).length !== 4 || Object.keys(payload).some(key => !['operation', 'root', 'home', 'path'].includes(key)))
    throw new Error('BFP worker accepts only the fixed verify operation');
  for (const key of ['root', 'home', 'path']) {
    if (typeof payload[key] !== 'string' || !isAbsolute(payload[key]) || payload[key].includes('\0') || Buffer.byteLength(payload[key]) > 4096)
      throw new Error(`invalid BFP ${key}`);
  }
  // No pack/extract/native-program endpoint. Root/home come from the trusted
  // server owner, never from the public tool's argument object.
  return await createV6Bfp({ root: payload.root, home: payload.home }).verify(payload.path, { signal });
});
