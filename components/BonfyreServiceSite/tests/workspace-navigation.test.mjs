import {test,expect} from 'bun:test';
import {workspaceRoute,workspacePath,workspaceLinks} from '../workspace-navigation.mjs';

test('workspace routing adapts to supplied domains and permissions without carrying an unauthorized workspace',()=>{
  const views=[{id:'cases',label:'Cases',available:w=>w.canRead},{id:'team',label:'Team',available:w=>w.canAdmin}];
  const workspaces=[{id:'north',canRead:true,canAdmin:false},{id:'south',canRead:true,canAdmin:true}];
  expect(workspaceRoute(new URL('https://example.test/desk/south/team'),'/desk',workspaces,views).view.id).toBe('team');
  const denied=workspaceRoute(new URL('https://example.test/desk/north/team'),'/desk',workspaces,views);
  expect(denied.view.id).toBe('cases');expect(denied.available.map(v=>v.id)).toEqual(['cases']);
  expect(workspaceRoute(new URL('https://example.test/desk/unknown/team'),'/desk',workspaces,views).workspace.id).toBe('north');
  expect(workspaceRoute(new URL('https://example.test/desk'),'/desk',[],views).workspace).toBeNull();
  expect(workspaceLinks('/desk',workspaces[0],views,'cases')).not.toContain('mode.team');
  expect(workspacePath('/desk','north','cases')).toBe('/desk/north/cases');
});
