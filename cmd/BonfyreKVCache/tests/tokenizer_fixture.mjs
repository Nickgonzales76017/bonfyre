// A complete byte-level BPE test vocabulary, not a pretrained model tokenizer.
// The production GigaToken bridge reads this normal Hugging Face carrier.
import {writeFileSync} from 'node:fs';
const bytes=[];
for(let i=33;i<=126;i++)bytes.push(i);
for(let i=161;i<=172;i++)bytes.push(i);
for(let i=174;i<=255;i++)bytes.push(i);
const visible=new Set(bytes), alphabet=new Map(bytes.map(b=>[b,b]));
let next=256;
for(let b=0;b<256;b++)if(!visible.has(b))alphabet.set(b,next++);
const vocab=Object.fromEntries([...alphabet].map(([byte,code])=>[String.fromCodePoint(code),byte]));
Object.assign(vocab,{th:256,the:257,an:258,and:259});
const tokenizer={version:'1.0',truncation:null,padding:null,added_tokens:[],normalizer:null,
  pre_tokenizer:{type:'ByteLevel',add_prefix_space:false,trim_offsets:true,use_regex:true},post_processor:null,
  decoder:{type:'ByteLevel',add_prefix_space:false,trim_offsets:true,use_regex:true},
  model:{type:'BPE',dropout:null,unk_token:null,continuing_subword_prefix:'',end_of_word_suffix:'',fuse_unk:false,byte_fallback:false,ignore_merges:false,vocab,merges:['t h','th e','a n','an d']}};
if(process.argv.length!==3)throw Error('usage: tokenizer_fixture.mjs OUTPUT');
writeFileSync(process.argv[2],JSON.stringify(tokenizer)+'\n',{flag:'wx',mode:0o600});
