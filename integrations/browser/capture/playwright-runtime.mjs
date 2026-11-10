import {createRequire} from 'node:module';
import {homedir} from 'node:os';
import {join} from 'node:path';

const runtimeRequire=createRequire(join(homedir(),'.bonfyre','bin','playwright-runtime.cjs'));
const runtime=runtimeRequire('@playwright/test');
if(!runtime?.chromium)throw Error('The Bonfyre browser-test runtime is unavailable.');
export const chromium=runtime.chromium;
