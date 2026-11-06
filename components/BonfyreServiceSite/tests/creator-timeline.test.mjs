import {test, expect} from 'bun:test';
import {gestureClip, sourceSelection} from '../creator-timeline-model.mjs';
const source = {sample_rate:8000,frames:16000};
const clip = {at:4000,from:2000,frames:8000,gain:.5,muted:false};
test('arrangement gestures preserve source frames and clamp to source and session edges', () => {
  expect(gestureClip(clip,source,'move',1200,800)).toEqual({...clip,at:5600});
  expect(gestureClip(clip,source,'move',-999999).at).toBe(0);
  expect(gestureClip(clip,source,'move',999999).at).toBe(232000);
  expect(gestureClip(clip,source,'start',1600)).toEqual({...clip,at:5600,from:3600,frames:6400});
  expect(gestureClip(clip,source,'start',-999999)).toEqual({...clip,at:2000,from:0,frames:10000});
  expect(gestureClip(clip,source,'end',999999).frames).toBe(14000);
  expect(gestureClip(clip,source,'end',-999999).frames).toBe(1);
  expect(clip).toEqual({at:4000,from:2000,frames:8000,gain:.5,muted:false});
});
test('source trim keeps the placement and refuses empty or overflowing regions', () => {
  expect(sourceSelection(clip,source,.5,1)).toEqual({...clip,from:4000,frames:4000});
  expect(sourceSelection(clip,source,5,9)).toEqual({...clip,from:15999,frames:1});
  expect(sourceSelection({...clip,at:239000},source,0,2).frames).toBe(1000);
  expect(()=>gestureClip(clip,source,'delete',1)).toThrow();
  expect(()=>gestureClip(clip,source,'move',NaN)).toThrow();
});
