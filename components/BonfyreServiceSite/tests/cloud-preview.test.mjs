import {test,expect} from 'bun:test';
import {publicationBranch} from '../cloud.mjs';
test('preview publication cannot select production or an ambiguous alias',()=>{
 expect(publicationBranch(undefined)).toBe('main');
 expect(publicationBranch('brief-studio-upgrade','production')).toBe('brief-studio-upgrade');
 for(const branch of ['main','production','feature/brief','UPPER','-brief','brief-','a'.repeat(29),'',null])expect(()=>publicationBranch(branch,'production')).toThrow();
});
