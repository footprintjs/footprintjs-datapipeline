/** LOCAL SEED: candidate retention backend, actual inactive Neo client writer, no live device/DB/model. */
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { join, resolve } from 'node:path';
import { mkdir, readdir, readFile, stat, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createFilesystemRepository, createFilesystemTraceStore, createRetainedFilesystemRepository } from '../../src/storage/filesystem.js';
import { canonical } from '../../src/validation.js';
import type { CommitWrite, Repository, TraceStore } from '../../src/ports.js';
const [neoPath, outputPath] = process.argv.slice(2); assert.ok(neoPath && outputPath);
const root = resolve(outputPath); await mkdir(root,{recursive:true,mode:0o700}); assert.equal((await readdir(root)).length,0);
const load = (path:string)=>import(pathToFileURL(join(resolve(neoPath),path)).href);
const { createPowerScaleClientWriter } = await load('src/index.ts');
const { createInfluxDbSink } = await load('node_modules/footprintjs-datapipeline/dist/influxdb/index.js');
const { explainWriterKey } = await load('examples/traceCause.ts');
async function size(path:string):Promise<Record<string,number>> {
  const result:Record<string,number>={};
  for(const item of await readdir(path,{withFileTypes:true})) {
    const child=join(path,item.name);
    if(item.isDirectory()) for(const [key,value] of Object.entries(await size(child))) result[key]=(result[key]??0)+value;
    else {const key=item.name.includes('sqlite')?'metadata':item.name.endsWith('.gz')?'compressed':'other';result[key]=(result[key]??0)+(await stat(child)).size;}
  } return result;
}
const reports=[];
for(const count of [50,256]) {
  let now=1000,captures=0,sends=0; const path=join(root,String(count));await mkdir(path,{mode:0o700});
  const options={enabled:true,directory:join(path,'retained'),retentionMs:100,clock:()=>now,requireSeparateFilesystem:false,onHealth(){}};
  let retained=createRetainedFilesystemRepository(options)!;
  const plain=createFilesystemRepository({directory:join(path,'plain')});
  const traces=createFilesystemTraceStore({directory:join(path,'traces'),compression:'gzip'});
  const records=new Map<string,{ref:any;bytes:string}>();let operations=0,causeChecks=0;
  const repository:Repository={read:ref=>retained.read(ref),readMany:refs=>retained.readMany(refs),findOperation:id=>retained.findOperation(id),async commit(write:CommitWrite){
    await plain.commit(write);const result=await retained.commit(write);assert.equal(result.status,'committed');operations++;
    for(const row of [write.batch,...write.observations,write.evidence]) records.set(canonical(row.ref),{ref:row.ref,bytes:canonical(row)});
    return result;
  }};
  const traceStore:TraceStore={async store(trace){const ref=await traces.store(trace);const reopened=await traces.read(ref);assert.deepEqual(reopened,trace);
    const record=trace.value as any;const keys=record.commitLog?.flatMap((commit:any)=>Object.keys(commit.overwrite))??[];
    const key=keys.reverse().find((value:string)=>/^point_\d+$/.test(value))??'progress';
    assert.deepEqual(await explainWriterKey(reopened!.value,key),await explainWriterKey(record,key));causeChecks++;return ref;
  }};
  const bodyText=JSON.stringify({client:Array.from({length:count},(_,i)=>({node:i%8,protocol:'smb2',class:'read',remote_addr:`192.0.${Math.floor(i/256)}.${i%256}`,remote_name:`LOCAL SEED ${i} ${'x'.repeat(110)}`,user:{id:i,name:`test ${'u'.repeat(90)}`},operation_rate:i,in:count-i,out:0,time_avg:(i*113)%count,time_max:(i*47)%count,num_operations:200,in_avg:100,in_max:200,out_avg:10,out_max:20,time_min:1}))});
  const at='2026-10-19T12:00:00.000Z';
  const writerOptions={repository,traceStore,config:{name:'LOCAL SEED',prefix:'ps',topN:50,sentinels:['25']},sourceRef:{id:'synthetic-storage-measurement',version:'1'},destinationRef:{id:'simulated-sink',version:'1'},clock:()=>at,pollClockNs:()=> '1792411200000000000',client:{origin:'https://synthetic.example',async readClients(){captures++;return {endpoint:'https://synthetic.example/platform/3/statistics/summary/client',requestStartedAt:at,receivedAt:at,bodyText,provenance:'LOCAL SEED'};}},sink:createInfluxDbSink({url:'http://synthetic.example',org:'LOCAL SEED',bucket:'simulated',getToken:()=> 'synthetic',fetch:async()=>{sends++;return new Response(null,{status:204});}})};
  const run={runId:`synthetic-${count}`,attemptId:'initial'};
  const result=await createPowerScaleClientWriter(writerOptions).run(run);assert.equal(result.delivery?.status,'acknowledged',JSON.stringify(result));
  for(const row of records.values()){assert.equal(canonical(await retained.read(row.ref)),row.bytes);assert.equal(canonical(await plain.read(row.ref)),row.bytes);}
  retained.close();retained=createRetainedFilesystemRepository(options)!;
  const recovered=await createPowerScaleClientWriter(writerOptions).run(run);assert.equal(recovered.outcome,'recovered');assert.equal(captures,1);assert.equal(sends,1);
  retained.close();const before=await size(options.directory);retained=createRetainedFilesystemRepository(options)!;now=1100;
  await assert.rejects(()=>retained.read(result.refs.capture),(error:any)=>error.code==='REPOSITORY_EXPIRED');
  const swept=await retained.sweep({limit:100});assert.equal(swept.deleted,operations);
  await assert.rejects(()=>retained.read(result.refs.capture),(error:any)=>error.code==='REPOSITORY_EXPIRED');
  retained.close();const after=await size(options.directory);
  reports.push({returnedRows:count,counts:result.counts,records:records.size,operations,causeChecks,captureAndSendCount:{captures,sends},plainBytes:await size(join(path,'plain')),retainedBeforeExpiry:before,retainedAfterExpiry:after,expiredReferenceStillExplicit:true});
}
const report={provenance:'LOCAL SEED; actual inactive writer and candidate repository; simulated sink',policy:'100 ms controlled-clock TTL ONLY for expiry proof, not a deployment recommendation',reports,limitations:['No live environment or sustained throughput proof','SQLite metadata/tombstones retained indefinitely and measured separately','Trace files are separate and not deleted by repository expiry']};
await writeFile(join(root,'report.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));
