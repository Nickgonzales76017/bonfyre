// Native template table from the exact browser component catalogue.
// This is a source compiler, never user-supplied template execution.
import {readFileSync,writeFileSync} from "node:fs";
import {fileURLToPath} from "node:url";
import {NATIVE_AUTHORING_TEMPLATES,NATIVE_EDITORIAL_TEMPLATES} from "../../../components/BonfyreServiceSite/authoring/native-component-templates.mjs";
const file=fileURLToPath(new URL("../../BonfyreWebAddress/generated/native_authoring_templates.h",import.meta.url));
const cString=value=>JSON.stringify(value).replace(/\\u2028/g,"\\342\\200\\250").replace(/\\u2029/g,"\\342\\200\\251");
export function generatedNativeTemplates(){
 const table=NATIVE_AUTHORING_TEMPLATES.map(x=>
  ' { '+cString(x.kind)+', '+cString(x.start)+', '+cString(x.end)+' },').join("\n");
 const editorial=NATIVE_EDITORIAL_TEMPLATES.map(x=>
  ' { '+cString(x.kind)+', '+cString(x.markup)+' },').join("\n");
 return [
  "/* GENERATED: source components/BonfyreServiceSite/authoring/native-component-templates.mjs.",
  " * The template list is trusted first-party code, never an editor/CMS template.",
  " * Native output needs exact ownership and publication admission upstream. */",
  "#ifndef BONFYRE_NATIVE_AUTHORING_TEMPLATES_H",
  "#define BONFYRE_NATIVE_AUTHORING_TEMPLATES_H",
  "#include <string.h>",
  "typedef struct {const char *kind,*prefix,*suffix;} BfNativeAuthoringTemplate;",
  "static const BfNativeAuthoringTemplate bf_native_authoring_templates[]={",
  table,
  "};",
  "static inline const BfNativeAuthoringTemplate *bf_native_authoring_template(const char *kind){",
  " if(!kind)return NULL;",
  " for(size_t i=0;i<sizeof(bf_native_authoring_templates)/sizeof(bf_native_authoring_templates[0]);i++)",
  "  if(!strcmp(kind,bf_native_authoring_templates[i].kind))return &bf_native_authoring_templates[i];",
  " return NULL;",
  "}",
  "typedef struct {const char *kind,*markup;} BfNativeEditorialTemplate;",
  "static const BfNativeEditorialTemplate bf_native_editorial_templates[]={",
  editorial,
  "};",
  "static inline const BfNativeEditorialTemplate *bf_native_editorial_template(const char *kind){",
  " if(!kind)return NULL;",
  " for(size_t i=0;i<sizeof(bf_native_editorial_templates)/sizeof(bf_native_editorial_templates[0]);i++)",
  "  if(!strcmp(kind,bf_native_editorial_templates[i].kind))return &bf_native_editorial_templates[i];",
  " return NULL;",
  "}",
  "#endif",""
 ].join("\n");
}
if(process.argv[1]&&fileURLToPath(import.meta.url)===process.argv[1]){
 const mode=process.argv[2],content=generatedNativeTemplates();
 if(mode==="--write"){writeFileSync(file,content);console.log("native authoring templates written");}
 else if(mode==="--check"){
  if(readFileSync(file,"utf8")!==content){console.error("native authoring template generation stale");process.exit(1);}
  console.log("native authoring template generation PASS");
 }else {console.error("use --write or --check");process.exit(2);}
}
