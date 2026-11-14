import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
export async function buildConnectedMeasurementApp(file=new URL('../../integrations/mcp/dist/connected-performance.html',import.meta.url)){
 const built=await Bun.build({entrypoints:[fileURLToPath(new URL('./connected-measurement-mcp-app.mjs',import.meta.url))],target:'browser',minify:true});
 if(!built.success||built.outputs.length!==1)throw new AggregateError(built.logs,'Connected performance app failed to build');
 const [template,source]=await Promise.all([readFile(new URL('./connected-measurement-mcp-app.html',import.meta.url),'utf8'),built.outputs[0].text()]);
 const html=template.replace('<!-- CONNECTED_MEASUREMENT_APP_SCRIPT -->',()=>'<script type="module">'+source.replace(/<\/script/gi,'<\\/script')+'</script>');
 if(Buffer.byteLength(html)>512*1024||!html.startsWith('<!doctype html>'))throw Error('Connected performance app exceeded its resource budget');
 const path=file instanceof URL?fileURLToPath(file):file;
 await mkdir(dirname(path),{recursive:true});await writeFile(path,html);
 return {path,bytes:Buffer.byteLength(html),sha256:createHash('sha256').update(html).digest('hex')};
}
if(import.meta.main)console.log(JSON.stringify(await buildConnectedMeasurementApp(process.argv[2])));
