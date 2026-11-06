// The provider wire is JSON-RPC. Retained work and revisions belong to SourceFrontier.
import {createHash} from 'node:crypto';
import {sourceAddress} from './source_material.mjs';
export class WebRefusal extends Error {constructor(message,code='invalid',uncertain=false){super(message);this.code=code;this.uncertain=uncertain;}}
const fail=(ok,message)=>{if(!ok)throw new WebRefusal(message);};
const hash=value=>createHash('sha256').update(value).digest('hex');
const protocolVersions=['2025-11-25','2025-06-18','2025-03-26'];
export const webProviders=Object.freeze({
 firecrawl:{name:'Firecrawl',endpoint:'https://mcp.firecrawl.dev/v2/mcp',search:'firecrawl_search',read:'firecrawl_scrape'},
 exa:{name:'Exa',endpoint:'https://mcp.exa.ai/mcp?tools=web_search_exa,web_fetch_exa,web_search_advanced_exa',search:'web_search_advanced_exa',read:'web_fetch_exa'}
});
const text=(value,name,max,required=false)=>{fail(typeof value==='string'&&value.length<=max&&!value.includes('\0')&&(!required||value.trim()),'Provide a bounded '+name+'.');return value.trim();};
const hosts=(values,name)=>{fail(Array.isArray(values)&&values.length<=12&&values.every(v=>typeof v==='string'&&v.length<=253&&/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9][a-z0-9-]*[a-z0-9]$/i.test(v)),'Use up to 12 hostnames for '+name+'.');return [...new Set(values.map(v=>v.toLowerCase()))];};
const day=value=>{fail(typeof value==='string'&&(!value||/^\d{4}-\d{2}-\d{2}$/.test(value)&&new Date(value).toISOString().slice(0,10)===value),'Use an exact publication date.');return value;};
export function webSpec(input){
 fail(input&&typeof input==='object'&&!Array.isArray(input),'Review one web operation.');
 const keys=['provider','operation','query','objective','limit','includeDomains','excludeDomains','startDate','endDate','country','location','category','strategy','characters','urls','channel'];
 fail(Object.keys(input).every(k=>keys.includes(k)),'The web request contains unsupported fields.');
 const p=input.provider,operation=input.operation;fail(Object.hasOwn(webProviders,p)&&['search','read'].includes(operation),'Choose Firecrawl or Exa and a supported web operation.');
 const spec={provider:p,operation,query:text(input.query??'','search query',1600,operation==='search'),objective:text(input.objective??'','research purpose',800,true),limit:input.limit??10,includeDomains:hosts(input.includeDomains??[],'included domains'),excludeDomains:hosts(input.excludeDomains??[],'excluded domains'),startDate:day(input.startDate??''),endDate:day(input.endDate??''),country:text(input.country??'','country',2),location:text(input.location??'','search location',160),category:text(input.category??'','category',24),strategy:input.strategy??'auto',characters:input.characters??5000,urls:[],channel:input.channel??'web'};
 fail(Number.isSafeInteger(spec.limit)&&spec.limit>=1&&spec.limit<=25,'Choose between one and 25 results.');
 fail(Number.isSafeInteger(spec.characters)&&spec.characters>=500&&spec.characters<=20000,'Choose a content bound between 500 and 20,000 characters.');
 fail(!spec.includeDomains.length||!spec.excludeDomains.length,'Include or exclude domains in one run, rather than mixing the two.');
 fail(!spec.startDate||!spec.endDate||spec.startDate<=spec.endDate,'The publication range ends before it starts.');
 fail(!spec.country||/^[A-Z]{2}$/.test(spec.country),'Use a two-letter uppercase country code.');
 fail(['auto','lite'].includes(spec.strategy),'Choose automatic or cached lightweight search.');
 fail(['web','news','images'].includes(spec.channel),'Choose web, news or images.');
 if(p==='firecrawl'){fail(!spec.country,'Use Firecrawl location text; the country-code control belongs to Exa.');fail(!spec.startDate&&!spec.endDate,'Exact date ranges are available through Exa.');fail(['','research','pdf','developer'].includes(spec.category),'Choose a supported Firecrawl source category.');fail(spec.strategy==='auto','Cached lightweight mode belongs to Exa.');}
 else {fail(!spec.location,'Use an Exa country code; free-text location belongs to Firecrawl.');fail(spec.channel==='web','Exa uses categories for news; image search is available through Firecrawl.');fail(['','company','publication','news','pdf','github','personal site','people','financial report'].includes(spec.category),'Choose a supported Exa category.');fail(!['company','people'].includes(spec.category)||!spec.startDate&&!spec.endDate,'Company and people categories do not support publication-date filters.');fail(spec.category!=='people'||!spec.excludeDomains.length,'Exa people search does not support excluded domains.');}
 if(operation==='read'){
  fail(Array.isArray(input.urls)&&input.urls.length===1,'Read one '+webProviders[p].name+' page per run to keep its retained text tied to one reviewed address.');spec.urls=input.urls.map(sourceAddress);
  fail(!spec.query&&!spec.includeDomains.length&&!spec.excludeDomains.length&&!spec.startDate&&!spec.endDate&&!spec.category&&!spec.country&&!spec.location,'Page reading uses exact URLs, not search filters.');
 }else fail(!input.urls||Array.isArray(input.urls)&&!input.urls.length,'Search accepts a question rather than target URLs.');
 return spec;
}
export function webRequest(spec){
 const s=webSpec(spec),provider=webProviders[s.provider];let args;
 if(s.operation==='read')args=s.provider==='firecrawl'?{url:s.urls[0],formats:['markdown'],onlyMainContent:true,removeBase64Images:true,timeout:12000}:{urls:s.urls,maxCharacters:s.characters};
 else if(s.provider==='firecrawl')args={query:s.query,limit:s.limit,sources:[s.channel],domainTools:false,highlights:true,...s.includeDomains.length?{includeDomains:s.includeDomains}:{},...s.excludeDomains.length?{excludeDomains:s.excludeDomains}:{},...s.category?{categories:[s.category]}:{},...s.location?{location:s.location}:{}};
 else args={query:s.query,numResults:s.limit,type:s.strategy,textMaxCharacters:s.characters,enableHighlights:true,highlightsMaxCharacters:2000,moderation:true,...s.includeDomains.length?{includeDomains:s.includeDomains}:{},...s.excludeDomains.length?{excludeDomains:s.excludeDomains}:{},...s.startDate?{startPublishedDate:s.startDate}:{},...s.endDate?{endPublishedDate:s.endDate}:{},...s.country?{userLocation:s.country}:{},...s.category?{category:s.category}:{}};
 return {url:provider.endpoint,name:provider[s.operation],arguments:args};
}
function responseValue(value,id){if(value?.jsonrpc!=='2.0'||value.id!==id)return null;if(value.error)throw new WebRefusal('The provider refused this operation. Review its connection and request limits.','provider_refused');if(!value.result)throw new WebRefusal('The provider returned no complete result.','incomplete');return value.result;}
async function rpc(request,url,headers,id,method,params,signal){
 let response;try{response=await request(url,{method:'POST',redirect:'error',headers:{...headers,'Content-Type':'application/json',Accept:'application/json, text/event-stream'},body:JSON.stringify({jsonrpc:'2.0',...(id===null?{}:{id}),method,...(params===undefined?{}:{params})}),signal});}catch{throw new WebRefusal('The provider reply was interrupted. This operation will not be repeated automatically.','transport',method==='tools/call');}
 if(!response.ok){try{await response.body?.cancel();}catch{}throw new WebRefusal(response.status===429?'The provider rate limit was reached. Keep this run and try a new reviewed run later.':response.status===401||response.status===403?'The provider requires a valid account for this operation.':'The provider did not accept this request.','http_'+response.status,response.status>=500&&method==='tools/call');}
 if(id===null){try{await response.body?.cancel();}catch{}if(response.status!==202)throw new WebRefusal('The provider did not acknowledge protocol initialization.','protocol');return;}
 const reader=response.body?.getReader();if(!reader)throw new WebRefusal('The provider response body is missing.','incomplete',true);
 const decoder=new TextDecoder('utf-8',{fatal:true});let body='',pending='',size=0,result=null,eventData=[];const sse=(response.headers.get('content-type')||'').includes('text/event-stream');
 const readEvents=()=>{let end;while((end=pending.indexOf('\n'))>=0){const line=pending.slice(0,end).replace(/\r$/,'');pending=pending.slice(end+1);if(!line){if(eventData.length){let packet;try{packet=JSON.parse(eventData.join('\n'));}catch{throw new WebRefusal('The provider returned invalid protocol data.','malformed',method==='tools/call');}eventData=[];result=responseValue(packet,id);if(result)break;}}else if(line.startsWith('data:'))eventData.push(line.slice(5).replace(/^ /,''));}};
 try{for(;;){const part=await reader.read();if(part.done){const tail=decoder.decode();body+=tail;if(sse){pending+=tail;readEvents();}break;}size+=part.value.length;if(size>4*1024*1024)throw new WebRefusal('The provider response exceeds the retained-source read limit.','oversize',true);const chunk=decoder.decode(part.value,{stream:true});body+=chunk;if(sse){pending+=chunk;readEvents();if(result){await reader.cancel();break;}}}
  if(!sse){let value;try{value=JSON.parse(body);}catch{throw new WebRefusal('The provider returned invalid protocol data.','malformed',true);}result=responseValue(value,id);}
  if(!result)throw new WebRefusal('The provider did not finish its response.','incomplete',true);
  return {result,raw:body,session:response.headers.get('mcp-session-id')};
 }catch(error){if(error instanceof WebRefusal)throw error;throw new WebRefusal('The provider reply ended before it could be retained.','transport',method==='tools/call');}finally{try{await reader.cancel();}catch{}reader.releaseLock();}
}
function exaReadText(spec,result){
 // Exa joins pages with unescaped Markdown headers. Only a single reviewed
 // URL can be attributed without guessing boundaries inside page content.
 if(spec.urls.length!==1)throw new WebRefusal('Read one Exa page per run.','unsupported_result');
 const content=result.content;
 if(!Array.isArray(content)||content.length!==1||content[0]?.type!=='text'||typeof content[0].text!=='string')throw new WebRefusal('Exa returned no single page report.','unsupported_result');
 const body=content[0].text;
 if(body==='No content found for the provided URL(s).')throw new WebRefusal('Exa found no page text for this address.','no_content');
 const header=/^# ([^\r\n]*)\nURL: ([^\r\n]+)(?:\nPublished: ([^\r\n]+))?(?:\nAuthor: ([^\r\n]+))?(?:\n\n|$)/.exec(body);
 let url;try{url=header&&sourceAddress(header[2]);}catch{}
 if(!header||url!==spec.urls[0])throw new WebRefusal('Exa did not identify the exact reviewed page. Its report cannot be attributed safely.','unsupported_result');
 if(!body.slice(header[0].length).trim())throw new WebRefusal('Exa returned page metadata without page text.','no_content');
 return {results:[{url,title:header[1],publishedDate:header[3]||'',author:header[4]||'',text:body}]};
}
function providerNotices(value){
 const input=[...(Array.isArray(value.notices)?value.notices:[]),...(typeof value.warning==='string'?[value.warning]:[])];
 const notices=[];let noticesTruncated=false;
 for(const entry of input){const message=typeof entry==='string'?entry:entry&&typeof entry.message==='string'?entry.message:'';if(!message.trim())continue;if(notices.length===16){noticesTruncated=true;break;}if(message.length>1000)noticesTruncated=true;notices.push(message.slice(0,1000));}
 return {notices,noticesTruncated};
}
export function webResults(spec,result){
 if(result.isError)throw new WebRefusal('The provider could not complete this request. Its original error response is not treated as source material.','tool_refused');
 let value=result.structuredContent;if(!value){for(const part of result.content||[])if(part.type==='text'&&typeof part.text==='string'){try{const parsed=JSON.parse(part.text);if(parsed&&typeof parsed==='object'){value=parsed;break;}}catch{}}}
 if(!value&&spec.provider==='exa'&&spec.operation==='read')value=exaReadText(spec,result);
 if(!value)throw new WebRefusal('The provider did not return a supported structured result.','unsupported_result');
 if(value.success===false)throw new WebRefusal('The provider reported that this operation failed.','tool_refused');
 let input=[];
 if(spec.provider==='exa'){input=value.results??value.data?.results;if(!Array.isArray(input))throw new WebRefusal('Exa returned no explicit result collection.','unsupported_result');}
 else if(spec.operation==='read'){const data=value.data??value;input=[{url:data.metadata?.sourceURL||data.metadata?.url||spec.urls[0],title:data.metadata?.title||'',text:data.markdown||'',publishedAt:data.metadata?.publishedTime||''}];}
 else {const data=value.data??value;input=Array.isArray(data)?data:data[spec.channel];}
 if(!Array.isArray(input))throw new WebRefusal('The provider result collection is malformed.','malformed');
 const items=[],seen=new Set();let refused=0;
 for(const [position,item]of input.entries()){
  if(items.length>=25)break;let url;try{url=sourceAddress(item.url||item.link||item.id||'');}catch{refused++;continue;}
  if(seen.has(url))continue;seen.add(url);const rawText=typeof item.text==='string'?item.text:typeof item.markdown==='string'?item.markdown:typeof item.description==='string'?item.description:typeof item.snippet==='string'?item.snippet:Array.isArray(item.highlights)?item.highlights.filter(x=>typeof x==='string').join('\n'):'';
  const max=spec.operation==='read'?120000:Math.max(spec.characters,12000),body=rawText.slice(0,max);
  let image='';try{if(item.imageUrl||item.image)image=sourceAddress(item.imageUrl||item.image);}catch{}
  items.push({id:hash(url),url,title:typeof item.title==='string'&&item.title.trim()?item.title.slice(0,500):new URL(url).hostname+new URL(url).pathname,body,bodySHA256:hash(body),truncated:rawText.length>max,kind:spec.operation==='read'?'provider-page-extraction':spec.channel==='images'?'image-search-result':'search-excerpt',position:position+1,publishedAt:String(item.publishedDate||item.publishedAt||item.date||'').slice(0,80),author:String(item.author||'').slice(0,300),image});
 }
 return {items,refused,requestId:typeof value.requestId==='string'?value.requestId.slice(0,200):'',reportedCredits:Number.isFinite(value.creditsUsed)?value.creditsUsed:null,returned:input.length,...providerNotices(value)};
}
export async function executeWeb(spec,{request=fetch,signal=AbortSignal.timeout(16000),headers={}}={}){
 const s=webSpec(spec),call=webRequest(s),safeHeaders={};
 for(const[name,value]of Object.entries(headers)){if(!['authorization','x-api-key'].includes(name.toLowerCase())||typeof value!=='string'||/[\r\n]/.test(value))throw new WebRefusal('The credential owner returned an unsupported header.','credential_boundary');safeHeaders[name]=value;}
 const init=await rpc(request,call.url,safeHeaders,1,'initialize',{protocolVersion:protocolVersions[0],capabilities:{},clientInfo:{name:'Aurekai SourceFrontier',version:'1.0'}},signal);
 const version=init.result.protocolVersion;
 if(!protocolVersions.includes(version)||!init.result.capabilities?.tools||typeof init.result.capabilities.tools!=='object'||Array.isArray(init.result.capabilities.tools))throw new WebRefusal('The provider did not negotiate a supported tool protocol.','protocol');
 safeHeaders['MCP-Protocol-Version']=version;
 if(init.session){if(!/^[\x21-\x7e]{1,300}$/.test(init.session))throw new WebRefusal('Invalid provider session.','protocol');safeHeaders['mcp-session-id']=init.session;}
 await rpc(request,call.url,safeHeaders,null,'notifications/initialized',undefined,signal);
 const answer=await rpc(request,call.url,safeHeaders,2,'tools/call',{name:call.name,arguments:call.arguments},signal);
 return {...webResults(s,answer.result),raw:answer.raw,rawSHA256:hash(answer.raw),observedAt:new Date().toISOString(),provider:s.provider,tool:call.name};
}
