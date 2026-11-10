// Public schema projection only. No credential database or stored value is read.
export function enrollmentFields(source,provider){
 if(typeof source!=='string'||source.length>1024*1024||!/^[a-z0-9_-]{1,64}$/.test(provider??''))return null;
 let declarations;try{declarations=JSON.parse(source);}catch{return null;}
 if(!Array.isArray(declarations)||declarations.length>512)return null;
 const matches=declarations.filter(row=>row?.provider===provider);
 if(matches.length!==1||!Array.isArray(matches[0].fields)||!matches[0].fields.length||matches[0].fields.length>32)return null;
 const fields=[],names=new Set();
 for(const field of matches[0].fields){
  if(!field||!/^[a-z][a-z0-9_]{0,63}$/.test(field.field??'')||names.has(field.field)||typeof field.label!=='string'||!field.label.trim()||field.label.length>120||field.label.includes('\0')||typeof field.sensitive!=='boolean'||typeof field.required!=='boolean'||field.kind!==undefined&&(typeof field.kind!=='string'||!/^[a-z0-9_-]{1,64}$/.test(field.kind)))return null;
  if(field.help!==undefined&&(typeof field.help!=='string'||field.help.length>400||field.help.includes('\0')))return null;
  names.add(field.field);fields.push({field:field.field,label:field.label,...(field.kind?{kind:field.kind}:{}),sensitive:field.sensitive,required:field.required,help:field.help||''});
 }
 return fields;
}
