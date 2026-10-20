import assert from 'node:assert/strict';
import { mkdtemp, cp, symlink, readFile, writeFile, readdir, stat, rm } from 'node:fs/promises';
import { join, resolve, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
assert.ok(process.argv[2] && process.argv[3], 'Pass an installed Neo PowerScale package directory and a new report path');
const original=resolve(process.argv[2]);
const library=resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const temp=await mkdtemp(join(tmpdir(),'top50-capacity-'));
const report:any={provenance:'LOCAL SEED; synthetic saved responses; no device, database or deployment',settings:{topN:50,sentinels:['25'],reportedPageLimit:100,clusters:14,intervalSeconds:5},profiles:[],limitations:['Not a real deployed-row-count measurement or service throughput benchmark.','Current writer max top-N is 25; experiment copy raises only that admission constant to 50. Product source unchanged.','Reported page-limit setting is not referenced in this repository collector and is not simulated as 100 rows or pages.','No 30-day dataset generated; day/month amounts are linear workload illustrations.']};
async function files(p:string):Promise<string[]>{const result:string[]=[];for(const e of await readdir(p,{withFileTypes:true})){const f=join(p,e.name);result.push(...e.isDirectory()?await files(f):[f]);}return result;}
async function sizes(p:string){let bytes=0,allocated=0,count=0;for(const f of await files(p)){const s=await stat(f);bytes+=s.size;allocated+=s.blocks*512;count++;}return{bytes,allocated,count};}
try{
 await cp(join(original,'src'),join(temp,'src'),{recursive:true});
 await writeFile(join(temp,'package.json'),'{"type":"module"}');
 await symlink(join(original,'node_modules'),join(temp,'node_modules'));
 const constants=join(temp,'src/clientWriter/contracts.ts');const text=await readFile(constants,'utf8');assert.ok(text.includes('MAX_CLIENT_TOP_N = 25;'));await writeFile(constants,text.replace('MAX_CLIENT_TOP_N = 25;','MAX_CLIENT_TOP_N = 50;'));
 const {createPowerScaleClientWriter:current}=await import(pathToFileURL(join(original,'src/index.ts')).href);
 const {createPowerScaleClientWriter:candidate}=await import(pathToFileURL(join(temp,'src/index.ts')).href);
 const {createFilesystemRepository,createFilesystemTraceStore}=await import(pathToFileURL(join(library,'src/storage/filesystem.ts')).href);
 const {createInfluxDbSink}=await import(pathToFileURL(join(library,'src/influxdb/index.ts')).href);
 for(const rows of [50,256]){
  const root=join(temp,'runs',String(rows)),at='2026-10-20T12:00:00.000Z';
  const bodyText=JSON.stringify({client:Array.from({length:rows},(_,i)=>({node:i%8,protocol:'smb2',class:'read',remote_addr:`192.0.2.${i}`,remote_name:`LOCAL SEED ${i} ${'x'.repeat(110)}`,user:{id:String(i),name:`test ${'u'.repeat(90)}`},operation_rate:i,in:rows-i,out:0,time_avg:(i*113)%rows,time_max:(i*47)%rows,num_operations:200,in_avg:100,in_max:200,out_avg:10,out_max:20,time_min:1}))});
  const plain=createFilesystemTraceStore({directory:join(root,'plain')}),gzip=createFilesystemTraceStore({directory:join(root,'gzip'),compression:'gzip'});const refs:any[]=[];
  const options={repository:createFilesystemRepository({directory:join(root,'data')}),traceStore:{async store(t:any){const a=await plain.store(t),b=await gzip.store(t);assert.deepEqual(a,b);refs.push(b);return b;}},sourceRef:{id:'LOCAL-SEED',version:'1'},destinationRef:{id:'simulated',version:'1'},config:{name:'LOCAL SEED',prefix:'ps',topN:50,sentinels:['25']},clock:()=>at,pollClockNs:()=>'1792497600000000000',client:{origin:'https://synthetic.example',async readClients(){return{endpoint:'https://synthetic.example/platform/3/statistics/summary/client',requestStartedAt:at,receivedAt:at,bodyText,provenance:'LOCAL SEED'};}},sink:createInfluxDbSink({url:'http://synthetic.example',org:'LOCAL SEED',bucket:'simulated',getToken:()=> 'synthetic',fetch:async()=>new Response(null,{status:204})})};
  if(rows===50){try{const refused=current(options);report.currentWriter=await refused.run({runId:'current-top50',attemptId:'initial'});}catch(e){report.currentWriter={status:'refused',name:(e as Error).name,message:(e as Error).message};}}
  const t=performance.now();const result=await candidate(options).run({runId:`candidate-${rows}`,attemptId:'initial'});const elapsedMs=performance.now()-t;assert.equal(result.receiptSaved,true);assert.equal(result.delivery?.status,'acknowledged');
  for(const ref of refs){assert.deepEqual(await gzip.read(ref),await plain.read(ref));const bytes=gunzipSync(await readFile(join(root,'gzip/traces',ref.id+'.json.gz')));assert.equal(createHash('sha256').update(bytes).digest('hex'),ref.id);}
  const data=await sizes(join(root,'data')),trace=await sizes(join(root,'gzip')),uncompressed=await sizes(join(root,'plain'));const fleetPollsDay=241920;
  report.profiles.push({returnedRows:rows,counts:result.counts,sourceResponseBytes:Buffer.byteLength(bodyText),data,compressedTraces:trace,uncompressedTraces:uncompressed,reopenedTraces:refs.length,elapsedMs,illustrativeFleetDataAndTraceGbDay:(data.bytes+trace.bytes)*fleetPollsDay/1e9,illustrativeFleetDataAndTraceTb30Days:(data.bytes+trace.bytes)*fleetPollsDay*30/1e12});
 }
 await writeFile(process.argv[3],JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));
}finally{await rm(temp,{recursive:true,force:true});}
