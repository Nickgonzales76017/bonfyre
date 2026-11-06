import {test,expect} from 'bun:test';
import {stopOwnedChrome} from '../capture/chrome-session.mjs';
function fixture(mode){let finish;const calls=[],proc={exitCode:null,exited:new Promise(resolve=>finish=resolve),kill(signal='SIGTERM'){calls.push(signal);if(mode==='normal'||mode==='stubborn'&&signal==='SIGKILL'){proc.exitCode=0;finish(0);}}};return{proc,calls,finish};}
test('a normally exiting owned browser is not force-killed',async()=>{const f=fixture('normal');await stopOwnedChrome(f.proc,{graceMs:5,killMs:5});expect(f.calls).toEqual(['SIGTERM']);});
test('a stuck owned browser receives a bounded escalation on the same child handle',async()=>{const f=fixture('stubborn');await stopOwnedChrome(f.proc,{graceMs:5,killMs:5});expect(f.calls).toEqual(['SIGTERM','SIGKILL']);});
test('unconfirmed termination refuses cleanup rather than waiting forever',async()=>{const f=fixture('unconfirmed');await expect(stopOwnedChrome(f.proc,{graceMs:5,killMs:5})).rejects.toThrow(/profile was retained/);expect(f.calls).toEqual(['SIGTERM','SIGKILL']);f.finish(0);});
test('already exited children are never signalled',async()=>{const f=fixture('normal');f.proc.exitCode=0;f.finish(0);await stopOwnedChrome(f.proc,{graceMs:5,killMs:5});expect(f.calls).toEqual([]);});
test('actual owned child that ignores SIGTERM is joined after SIGKILL',async()=>{
 const child=Bun.spawn([process.execPath,'-e',"process.on('SIGTERM',()=>{});console.log('ready');setInterval(()=>{},1000);"],{stdout:'pipe',stderr:'ignore'});
 const reader=child.stdout.getReader();await reader.read();reader.releaseLock();
 try{await stopOwnedChrome(child,{graceMs:15,killMs:2000});expect(child.signalCode).toBe('SIGKILL');}
 finally{if(child.exitCode===null&&!child.signalCode){child.kill('SIGKILL');await child.exited;}}
});
