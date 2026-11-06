// A physical projection contract retained inside an existing editor release.
// It names only paths and record selectors; it cannot provide records or grants.
import {continuationFields as keys,validateContinuationViews} from '../integrations/browser/workspace-continuation.mjs';
export const workspaceNavigation='__WORKSPACE_NAV__';
// Shared document entrance, not an instrument theme: classed/id-owned canvases
// retain their own domain presentation. The native navigation stays common.
export const workspaceNavigationCSS=`
[data-native-workspace]{display:flex;flex-wrap:wrap;gap:10px 18px;align-items:center;min-height:70px;padding:14px clamp(18px,3vw,46px);font:500 13px/1.45 system-ui;box-sizing:border-box;overflow-wrap:anywhere;background:#17392d;color:#f4f8e9;border-bottom:1px solid #7da083;box-shadow:0 4px 22px #12372215}
[data-native-workspace] a{color:inherit;display:inline-flex;align-items:center;min-height:44px;padding:0 13px;border:1px solid #cfebc33d;border-radius:999px;text-decoration:none;transition:background .18s,border-color .18s}
[data-native-workspace] a:hover,[data-native-workspace] a:focus-visible{background:#d7f1c51a;border-color:#c1dfa9}[data-native-workspace] a:focus-visible{outline:3px solid #e5b75a;outline-offset:3px}[data-native-workspace] strong{max-width:100%;font-weight:650}
[data-native-workspace]+main:not([class]):not([id]){box-sizing:border-box;max-width:1640px;margin:auto;padding:clamp(22px,4vw,64px);color:#193c2c;background:linear-gradient(145deg,#f7f9f1,#edf3e7);min-height:calc(100vh - 72px)}
[data-native-workspace]+main:not([class]):not([id])>nav{gap:9px;margin-bottom:clamp(24px,4vw,52px)}
[data-native-workspace]+main:not([class]):not([id])>nav a{display:inline-flex;align-items:center;min-height:44px;padding:0 18px;border:1px solid #bbcbb4;border-radius:999px;text-decoration:none;background:#ffffffb8;font:600 13px system-ui;transition:background .2s,box-shadow .2s}
[data-native-workspace]+main:not([class]):not([id])>nav a:hover{background:#fff;box-shadow:0 5px 16px #15372114}
[data-native-workspace]+main:not([class]):not([id])>h1{font:400 clamp(43px,7vw,88px)/1.01 Georgia,serif;letter-spacing:-.055em;max-width:16ch;margin:0 0 clamp(26px,4vw,48px)}
[data-native-workspace]+main:not([class]):not([id])>form{box-sizing:border-box;display:grid;gap:22px;max-width:920px;padding:clamp(24px,4vw,50px);border:1px solid #557861;border-radius:24px;color:#f3f9e9;background:radial-gradient(ellipse at 100% 0,#90be7a45,transparent 62%),linear-gradient(125deg,#193d2f,#335d41);box-shadow:0 18px 50px #15372118}
[data-native-workspace]+main:not([class]):not([id])>form>label{font:600 clamp(19px,2vw,27px)/1.35 system-ui;gap:19px}
[data-native-workspace]+main:not([class]):not([id])>form>span{font:400 14px/1.7 system-ui;max-width:66ch;color:#d6e7d2}
[data-native-workspace]+main:not([class]):not([id])>form input[type=file]{box-sizing:border-box;width:100%;min-width:0;max-width:100%;padding:18px;border:1px dashed #a0bd92;border-radius:14px;background:#ffffff0c;color:#eef7e5;font:400 14px/1.6 system-ui}
[data-native-workspace]+main:not([class]):not([id])>form input::file-selector-button{border:0;border-radius:9px;padding:11px 15px;margin-right:14px;color:#193c2c;background:#e1edcf;font:650 13px system-ui;cursor:pointer}
[data-native-workspace]+main:not([class]):not([id])>form>button{justify-self:start;min-height:48px;padding:12px 25px;border:1px solid #cee4b4;border-radius:999px;background:#d7eab8;color:#183723;font:650 14px system-ui;cursor:pointer;transition:background .2s,box-shadow .2s}
[data-native-workspace]+main:not([class]):not([id])>form>button:hover{background:#e5f3cf;box-shadow:0 7px 20px #0a201833}[data-native-workspace]+main>form :focus-visible{outline:3px solid #e5b75a;outline-offset:4px}
[data-native-workspace]+main:not([class]):not([id])>form[hidden]{display:none}[data-native-workspace]+main:not([class]):not([id]):has(>form[hidden])>h1{font-size:clamp(28px,4vw,46px);margin-bottom:24px}
[data-native-workspace]+main:not([class]):not([id])>[role=status],[data-native-workspace]+main:not([class]):not([id])>[role=alert]{font:500 14px/1.6 system-ui;overflow-wrap:anywhere}[data-native-workspace]+main:not([class]):not([id])>[role=alert]{color:#943b25}
@media(max-width:600px){[data-native-workspace]{gap:7px;padding:12px 16px}[data-native-workspace] a{padding:0 10px}[data-native-workspace]+main:not([class]):not([id]){padding:24px 16px}[data-native-workspace]+main:not([class]):not([id])>form{padding:23px;border-radius:18px}[data-native-workspace]+main:not([class]):not([id])>form input[type=file]{padding:12px}}
@media(prefers-reduced-motion:reduce){[data-native-workspace] a,[data-native-workspace]+main:not([class]):not([id])>nav a,[data-native-workspace]+main:not([class]):not([id])>form>button{transition:none}}
`;
export function continuationManifest(views,files){
 validateContinuationViews(views);if(!(files instanceof Map))throw Error('Provide the reviewed editor release.');
 const rows=[];
 for(const view of views){
  const html=files.get(view.entry+'index.html');
  if(!html||!Buffer.from(html).toString('utf8').includes('bonfyre-workspace-store'))throw Error('Continuation entry is not a bound editor in this release.');
  rows.push(keys.map(k=>view[k]).join('\t'));
 }
 const result=Buffer.from(keys.join('\t')+'\n'+rows.join('\n')+'\n');if(result.length>32768)throw Error('Continuation descriptor exceeds its bound.');return result;
}
