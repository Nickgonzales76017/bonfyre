// Import each published Aurekai Journal Markdown source as one complete,
// editable native authoring document. Never extract a title and discard the
// article: original bytes and frontmatter remain in the typed document.
import {readdirSync,readFileSync,writeFileSync,mkdirSync} from "node:fs";
import {basename,join,resolve} from "node:path";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
import {newAuthoringTree,validateAuthoringTree} from "../runtime/authoring-tree.mjs";
import {encodeNativeAuthoring,decodeNativeAuthoring} from "../runtime/authoring-carrier.mjs";
const root=resolve(fileURLToPath(new URL("../../..",import.meta.url)));
const input=resolve(root,"editorial/posts");
const hash=content=>createHash("sha256").update(content).digest("hex");
const slugPattern=/^[a-z0-9][a-z0-9-]{1,110}$/;
export function journalArticles(dir=input){
 return readdirSync(dir).filter(x=>x.endsWith(".md")).sort();
}
function metadata(text){
 if(!text.startsWith("---\n"))throw Error("journal missing YAML frontmatter");
 const end=text.indexOf("\n---\n",4);
 if(end<0)throw Error("journal missing closing frontmatter");
 const header=text.slice(4,end).split("\n"),record={};
 for(const line of header){
  const match=/^([a-z][a-z0-9_]*):\s*(.*)$/.exec(line);
  if(!match)continue;
  let value=match[2].trim();
  if((value.startsWith('"')&&value.endsWith('"'))||
     (value.startsWith("'")&&value.endsWith("'")))
   value=value.slice(1,-1);
  record[match[1]]=value;
 }
 if(!record.title||!record.slug||!record.description)
  throw Error("source editorial metadata is incomplete");
 return record;
}
export function authoringFromJournal(path,contents){
 const source=Buffer.isBuffer(contents)?contents:Buffer.from(contents,"utf8");
 const raw=source.toString("utf8");
 if(!Buffer.from(raw,"utf8").equals(source))throw Error("non-UTF8 Markdown input");
 const front=metadata(raw);
 const name=basename(path,".md");
 if(!slugPattern.test(name)||name!==front.slug)throw Error("file identity and source slug differ: "+path);
 const tree=newAuthoringTree("aurekai-journal","/blog/"+name+"/");
 const digest=hash(source);
 tree.blocks=[
  {id:name+"-hero",kind:"profile-hero",
   props:{heading:front.title,body:front.description,eyebrow:"AUREKAI JOURNAL / "+(front.category||"Editorial"),
    variant:"editorial",
    metadata:{source:"editorial/posts",sha256:digest,publishing_status:"review-required",category:front.category||"Editorial"}},
   responsive:{desktop:{layout:"feature",width:"wide",typography:"serif",padding:32},
    phone:{layout:"stack",width:"full",typography:"serif",padding:12}},children:[]},
  {id:name+"-content",kind:"profile-story",
   props:{body:raw,heading:"The full investigation",
    source_ref:"editorial/posts/"+name+".md",source_digest:digest,
    metadata:{format:"markdown",frontmatter:true,slug:name,source_bytes:source.length,source_encoding:"UTF-8"}},
   responsive:{desktop:{layout:"stack",width:"medium",typography:"editorial",padding:18},
    phone:{layout:"stack",width:"full",typography:"serif",padding:0}},children:[]}
 ];
 return validateAuthoringTree(tree);
}
// Admission is the destination CMS owner's responsibility. This pure plan
// explicitly copies a public editorial source to a chosen draft namespace;
// it cannot grant publication rights or silently impersonate source ownership.
export function stageJournalDraftForNamespace(source,namespace){
 validateAuthoringTree(source);
 if(source.site_id!=="aurekai-journal"||
    !/^[-a-z0-9]{2,95}$/.test(namespace))
  throw Error("Journal source/destination identity is not admissible");
 const doc=structuredClone(source);
 doc.site_id=namespace;
 const hero=doc.blocks.find(x=>x.kind==="profile-hero");
 if(!hero)throw Error("Source article has no hero");
 hero.props.metadata={...hero.props.metadata,
  imported_from:"aurekai-journal",review:"required",publication:"not-granted"};
 return validateAuthoringTree(doc);
}
export function extractJournalSource(tree){
 validateAuthoringTree(tree);
 if(tree.blocks.length!==2||
   (tree.site_id!=="aurekai-journal"&&
    !tree.blocks.some(x=>x.props.metadata?.imported_from==="aurekai-journal")))
  throw Error("not an imported Journal document");
 const content=tree.blocks.find(x=>x.kind==="profile-story"&&
  x.props.metadata?.format==="markdown");
 if(!content||content.kind!=="profile-story"||content.props.metadata?.format!=="markdown")
  throw Error("source Markdown is unavailable");
 return Buffer.from(content.props.body,"utf8");
}
export function exportAllJournal({posts=input,out}={}){
 if(!out)throw Error("provide a named output directory");
 mkdirSync(out,{recursive:true});
 const files=journalArticles(posts);
 if(files.length!==90)throw Error("expected 90 source-owned Journal essays; found "+files.length);
 let manifest="slug\tsource_sha256\tcarrier_sha256\tbytes\turl\n";
 for(const file of files){
  const source=readFileSync(join(posts,file)),slug=basename(file,".md");
  const tree=authoringFromJournal(file,source);
  const bytes=encodeNativeAuthoring(tree);
  const decoded=decodeNativeAuthoring(bytes),round=extractJournalSource(decoded);
  if(!round.equals(source))throw Error("not source-identical after native serialization: "+slug);
  writeFileSync(join(out,slug+".bfau"),bytes);
  manifest+=slug+"\t"+hash(source)+"\t"+hash(bytes)+"\t"+source.length+"\t"+tree.page+"\n";
 }
 writeFileSync(join(out,"SOURCE.tsv"),manifest);
 return {articles:files.length,manifest,bytes:manifest.length};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const out=resolve(process.argv[2]||join(root,"cmd/BonfyreSurfaceScene/build/journal-import"));
 const result=exportAllJournal({out});
 console.log("AUREKAI_90_JOURNAL_SOURCE_ROUNDTRIP_PASS",result.articles,out);
}
