// Metadata is a projection of authored site content, never invented proof.
const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const json=v=>JSON.stringify(v).replace(/[<>&]/g,c=>String.fromCharCode(92)+'u'+c.charCodeAt(0).toString(16).padStart(4,'0'));
export function publicOrigin(site){
 if(!site.public_origin)return null;const u=new URL(site.public_origin);
 if(u.protocol!=='https:'||u.username||u.password||u.port||u.pathname!=='/'||u.search||u.hash)throw Error('public_origin must be an HTTPS origin');return u.origin;
}
export function metadata(site,page){
 const origin=publicOrigin(site);if(!origin)return '';
 const url=origin+'/'+(page.path?page.path+'/':''),title=page.path?page.title+' | '+site.business.name:site.business.page_title??site.business.name,description=page.description??site.business.description??'';
 const logo=site.business.logo;const image=logo&&/^assets\/[a-zA-Z0-9._/-]+$/.test(logo)?origin+'/'+logo:null;
 const org={'@context':'https://schema.org','@type':'Organization',name:site.business.name,url:origin+'/',...(image?{logo:image}:{}),...(site.contact.phone?{telephone:site.contact.phone}:{}),...(site.contact.email?{email:site.contact.email}:{}),...(site.footer?.areas?.items?{areaServed:site.footer.areas.items}:{})};
 return '<link rel="canonical" href="'+escape(url)+'" />\n<meta property="og:type" content="website" />\n<meta property="og:url" content="'+escape(url)+'" />\n<meta property="og:title" content="'+escape(title)+'" />\n<meta property="og:description" content="'+escape(description)+'" />\n<meta property="og:site_name" content="'+escape(site.business.name)+'" />\n<meta name="twitter:card" content="summary" />'+(image?'\n<meta property="og:image" content="'+escape(image)+'" />':'')+'\n<script type="application/ld+json">'+json(org)+'</script>';
}
export function discovery(site,pages){
 const origin=publicOrigin(site);if(!origin)return null;
 const urls=pages.map(p=>origin+'/'+(p.path?p.path+'/':''));return {robots:'User-agent: *\n'+(site.preview?'Disallow: /':'Allow: /')+'\nSitemap: '+origin+'/sitemap.xml\n',sitemap:'<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'+urls.map(u=>'<url><loc>'+escape(u)+'</loc></url>').join('')+'</urlset>\n'};
}
