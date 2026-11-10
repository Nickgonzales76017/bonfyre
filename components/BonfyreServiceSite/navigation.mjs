// Optional authored navigation and actions remain local, data-only links.
export function localHref(value) {
 if(typeof value!=='string'||!(/^(?:\/(?!\/)[a-z0-9/-]*|)#[a-z][a-z0-9-]*$/.test(value)||/^\/(?!\/)[a-z0-9/-]*$/.test(value)))throw Error('Site link must be a local page or named anchor');
 if(value.includes('//')||value.includes('..'))throw Error('Invalid local link');return value;
}
// The site menu may also point at one of the estate's own addresses: a
// territory, the Work Samples deployment or the owner's GitHub Pages directory.
export function siteHref(value) {
 if(typeof value==='string'&&/^https:\/\/(?:(?:[a-z0-9-]+\.)?aurekai-work-samples\.pages\.dev|nickgonzales76017\.github\.io)\/[a-z0-9/-]*$/.test(value)&&!value.includes('/../'))return value;
 if(typeof value==='string'&&/^https:\/\/(?:[a-z0-9-]+\.)?aurekai\.(?:life|live|shop|work)\/[a-z0-9/-]*$/.test(value))return value;
 return localHref(value);
}
export function checkLinks(pages){
 const known=new Map(pages.map(p=>['/'+(p.path?p.path+'/':''),new Set([...p.html.matchAll(/\bid="([^"]+)"/g)].map(m=>m[1]))]));
 for(const p of pages){const own='/'+(p.path?p.path+'/':'');for(const m of p.html.matchAll(/<a\b[^>]*\bhref="([^"<>]+)"/g)){
  const link=m[1];if(!link.startsWith('#')&&!link.startsWith('/'))continue;if(link.startsWith('/assets/'))continue;
  const [pathname,fragment]=localHref(link).split('#'),path=pathname||own;
  if(!known.has(path)||fragment&&!known.get(path).has(fragment))throw Error('Dead site link '+own+' -> '+link);
 }}
}
