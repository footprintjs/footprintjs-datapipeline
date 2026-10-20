import { afterEach, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { RuntimeSnapshot } from 'footprintjs';
import { createPipeline, createRunWindowEvidenceTransform, RUN_RECEIPT_SCHEMA, RUN_CONTRIBUTION_SCHEMA, type CandidateBatch, type NumericMetricDefinition, type Observation, type DeriveRequest, type JsonObject, type JsonValue } from '../src/index.js';
import { createFilesystemRepository } from '../src/storage/filesystem.js';
function object(value: unknown): JsonObject {
 expect(value !== null && typeof value === 'object' && !Array.isArray(value)).toBe(true);
 return value as JsonObject;
}
function changedPayload(observation: Observation, payload: JsonValue): Observation {
 const { externalArtifactRef: _artifact, ...inline } = observation;
 return { ...inline, payload };
}
const dirs: string[]=[];
afterEach(async()=>{await Promise.all(dirs.splice(0).map(p=>rm(p,{recursive:true,force:true})));});
const start='2026-10-20T12:00:00.000Z', end='2026-10-20T12:05:00.000Z';
const source={ref:{id:'LOCAL-SEED',version:'1'},adapterRef:{id:'test-runs',version:'1'}};
export const metric:NumericMetricDefinition={sourceRef:source.ref,metricRef:{id:'sample-average',version:'1'},unit:'us',grain:'retained-poll-average',weighting:'equal-per-sample',clock:'collection',dimensions:{client:'192.0.2.1'},populationRef:{id:'top-n',version:'1'},retentionPolicyRef:{id:'test-only-no-deletion',version:'1'},gtThresholds:[25]};
export async function fixture(slots=[0,2], status='complete', windowSeconds=300) {
 const windowEnd=new Date(Date.parse(start)+windowSeconds*1000).toISOString();
 const directory=await mkdtemp(join(tmpdir(),'run-window-'));dirs.push(directory);
 const repository=createFilesystemRepository({directory:join(directory,'data')});
 const traces:RuntimeSnapshot[]=[];
 const options={metric,anchorAt:start,intervalMs:5000,traceStore:{async store(trace: {value:unknown}){traces.push(trace.value as RuntimeSnapshot);return {kind:'trace' as const,id:`native-${traces.length}`};}}};
 const transform=createRunWindowEvidenceTransform({...options,enabled:true})!;
 let now=start;
 const pipeline=createPipeline({sources:[source],repository,transforms:[transform],clock:()=>now,adapters:[{ref:source.adapterRef,async collect(_s,request):Promise<CandidateBatch>{
  const slot=Number(request);const at=new Date(Date.parse(start)+slot*5000).toISOString();
  return {key:`run-${slot}`,outcome:'complete',coverage:{status:'unknown',time:{kind:'source-time-unknown'}},observations:[
   {key:'receipt',subject:{},schemaRef:RUN_RECEIPT_SCHEMA,time:{kind:'source-time-unknown'},collectedAt:at,payload:{slotAt:at,status,rawDisposition:'not-retained',retentionPolicyRef:{...metric.retentionPolicyRef}}},
   ...(status==='failed'?[]:[{key:'input',subject:metric.dimensions,schemaRef:RUN_CONTRIBUTION_SCHEMA,time:{kind:'source-time-unknown' as const},collectedAt:at,payload:{receiptKey:'receipt',metric:JSON.parse(JSON.stringify(metric)),status:'valid',value:slot===0?20:40}}]),
  ]};}}]});
 const refs=[];for(const slot of slots){now=new Date(Date.parse(start)+slot*5000+1000).toISOString();const result=await pipeline.collect({sourceRef:source.ref,idempotencyKey:`run-${slot}`,request:slot});expect(result.commit.status).toBe('committed');refs.push(result.commit.batchRef!);}
 now=windowEnd;
 const request={methodRef:transform.ref,idempotencyKey:'window',inputBatchRefs:refs,window:{basis:'collection' as const,start,end:windowEnd},request:{knowledgeCutoff:windowEnd}};
 return {pipeline,repository,options,transform,request,traces,directory};
}
export async function summary(f:Awaited<ReturnType<typeof fixture>>) {const result=await f.pipeline.derive(f.request);expect(result.commit.status).toBe('committed');const value=await f.pipeline.read(result.commit.observationRefs![0]!);expect(value?.kind).toBe('observation');return value as Observation;}
it('is absent by default and cannot alter existing callers',()=>{expect(createRunWindowEvidenceTransform({metric,anchorAt:start,intervalMs:5000})).toBeUndefined();});
it('saves an exact-input summary, keeps 58 missing polls as gaps, and reopens unchanged',async()=>{
 const f=await fixture();const saved=await summary(f);expect(saved.payload).toMatchObject({statistics:{count:2,mean:30},coverage:{expectedPolls:60,receivedPolls:2,missingPolls:58,continuous:'unknown'}});
 expect((object(saved.payload).gaps as JsonValue[])).toHaveLength(58);expect(saved.support.kind).toBe('inputs');
 if(saved.support.kind==='inputs')for(const ref of saved.support.inputRefs)expect(await f.pipeline.read(ref)).toBeDefined();
 const reopened=createFilesystemRepository({directory:join(f.directory,'data')});expect(await reopened.read(saved.ref)).toEqual(saved);
 expect(f.traces).toHaveLength(1);expect(f.traces[0]!.sharedState.statistics).toMatchObject({count:2,mean:30});
});
it('keeps a failed run as an explicit gap without removing other statistics',async()=>{const f=await fixture([0],'failed');const saved=await summary(f);expect(saved.payload).toMatchObject({statistics:{count:0,mean:null},coverage:{failedPolls:1,missingPolls:59}});expect((object(saved.payload).gaps as JsonValue[]).some(g=>object(g).reason==='unavailable')).toBe(true);});
it('rejects a changed cutoff under the same operation identity',async()=>{const f=await fixture();await summary(f);const result=await f.pipeline.derive({...f.request,request:{knowledgeCutoff:'2026-10-20T12:06:00.000Z'}});expect(result.commit.status).toBe('failed');});
// These capacity cases perform hundreds of durable filesystem publications.
// Their budget covers fixture ingestion, not a relaxed product/performance assertion.
it('all sixty polls fit the native record bound without inventing coverage',async()=>{const f=await fixture(Array.from({length:60},(_,i)=>i));const saved=await summary(f);expect(saved.payload).toMatchObject({statistics:{count:60},coverage:{expectedPolls:60,receivedPolls:60,missingPolls:0,continuous:'unknown'}});expect(Buffer.byteLength(JSON.stringify(f.traces[0]))).toBeLessThan(6*1024*1024);}, 30_000);

async function loaded(f: Awaited<ReturnType<typeof fixture>>) {
 return Promise.all(f.request.inputBatchRefs.map(async ref => {
  const batch = await f.repository.read(ref);
  if(batch?.kind!=='batch') throw Error('Expected saved batch');
  const observations=await Promise.all(batch.observationRefs.map(ref=>f.repository.read(ref)));
  return {batch,observations:observations as Observation[]};
 }));
}
async function apply(f: Awaited<ReturnType<typeof fixture>>, inputs: Awaited<ReturnType<typeof loaded>>, request: DeriveRequest=f.request) {
 return f.transform.apply(inputs,request,{signal:new AbortController().signal,operationId:'isolated-evidence-check',startedAt:end,now:()=>end});
}
it('uses a recorded zero rather than treating it as a missing poll',async()=>{
 const f=await fixture([0]);const inputs=await loaded(f);const sample=inputs[0]!.observations[1]!;
 inputs[0]!.observations[1]=changedPayload(sample,{...object(sample.payload),value:0});
 const value=await apply(f,inputs);expect(value.observations[0]!.payload).toMatchObject({statistics:{count:1,mean:0},coverage:{missingPolls:59}});
});
it('keeps partial-poll values usable while completeness remains unresolved',async()=>{
 const f=await fixture([0],'partial');const value=await summary(f);expect(value.payload).toMatchObject({statistics:{count:1,mean:20},coverage:{partialPolls:1,missingPolls:59}});expect((object(value.payload).gaps as JsonValue[])).toHaveLength(60);
});
it('does not turn a complete receipt with no series contribution into a zero',async()=>{
 const f=await fixture([0]);const inputs=await loaded(f);inputs[0]!.observations.splice(1);
 const value=await apply(f,inputs);expect(value.observations[0]!.payload).toMatchObject({statistics:{count:0,mean:null},coverage:{receivedPolls:1,missingPolls:59}});expect((object(value.observations[0]!.payload).gaps as JsonValue[])).toHaveLength(60);
});
it('with the same cutoff, later-learned evidence stays excluded',async()=>{
 const f=await fixture();const inputs=await loaded(f);const known=await apply(f,inputs);
 const late={...inputs[0]!,batch:{...inputs[0]!.batch,finishedAt:'2026-10-20T12:06:00.000Z'}};
 const before=await apply(f,[late,inputs[1]!]);const repeated=await apply(f,[late,inputs[1]!]);
 expect(before.observations).toEqual(repeated.observations);expect(before.observations[0]!.payload).toMatchObject({statistics:{count:1,mean:40}});
 expect(known.observations[0]!.payload).toMatchObject({statistics:{count:2,mean:30}});
 const after=await apply(f,[late,inputs[1]!],{...f.request,request:{knowledgeCutoff:'2026-10-20T12:06:00.000Z'}});
 expect(after.observations[0]!.payload).toMatchObject({statistics:{count:2,mean:30}});
});
for(const mutation of ['duplicate-slot','foreign-source','foreign-receipt','wrong-unit','out-of-slot','over-limit','unbounded-parameter'] as const) it(`refuses ${mutation} with fixed diagnostic text`,async()=>{
 const f=await fixture();const inputs=await loaded(f);let request: DeriveRequest=f.request;
 if(mutation==='duplicate-slot') inputs[1]!.observations[0]=changedPayload(inputs[1]!.observations[0]!,inputs[0]!.observations[0]!.payload!);
 if(mutation==='foreign-source') inputs[0]={...inputs[0]!,batch:{...inputs[0]!.batch,sourceRef:{id:'other-source',version:'1'}}};
 const sample=inputs[0]!.observations[1]!;
 if(mutation==='foreign-receipt') inputs[0]!.observations[1]=changedPayload(sample,{...object(sample.payload),receiptKey:'private-secret-marker'});
 if(mutation==='wrong-unit') inputs[0]!.observations[1]=changedPayload(sample,{...object(sample.payload),metric:JSON.parse(JSON.stringify({...metric,unit:'ms'}))});
 if(mutation==='out-of-slot') inputs[0]!.observations[1]={...sample,collectedAt:'2026-10-20T12:01:00.000Z'};
 if(mutation==='over-limit') request={...request,window:{...request.window,end:'2026-10-20T12:10:00.000Z'},request:{knowledgeCutoff:'2026-10-20T12:10:00.000Z'}};
 if(mutation==='unbounded-parameter') request={...request,request:{knowledgeCutoff:end,query:'private-secret-marker'}};
 await expect(apply(f,inputs,request)).rejects.toMatchObject({code:'RUN_WINDOW_INVALID',message:'Saved run/window evidence was refused.'});
});

it('cause tools follow statistics to the exact saved contribution and its receipt',async()=>{
 const {findLastWriter,keysReadFromExecutionTree,readRequirementResults}=await import('foottrace');
 const {createRecordGraph}=await import('foottrace/cause');
 const f=await fixture();const saved=await summary(f);const record=JSON.parse(JSON.stringify(f.traces[0])) as RuntimeSnapshot;
 const reads=keysReadFromExecutionTree(record.executionTree);
 const control=record.recorders!.find(r=>r.name==='control-dependencies')!.data as {controlDeps:Record<string,{deciderId:string}>};
 const graph=createRecordGraph([{address:record.logAddress!,commitLog:record.commitLog,complete:true,controlDeps:control.controlDeps,
  reads:Object.fromEntries(record.commitLog.map(c=>[c.runtimeStageId,reads.lookup(c.runtimeStageId)]))}]);
 const node=(key:string)=>{const writer=findLastWriter(record.commitLog,key)!;return graph.nodeId(record.logAddress!,{key,commitIdx:record.commitLog.indexOf(writer),runtimeStageId:writer.runtimeStageId});};
 const context={signal:new AbortController().signal};
 const parents=await graph.parents(node('statistics'),context);
 expect(parents.edges).toContainEqual(expect.objectContaining({nodeId:node('input_0'),kind:'data',attribution:'per-write'}));
 const inputParents=await graph.parents(node('input_0'),context);
 expect(inputParents.edges).toContainEqual(expect.objectContaining({nodeId:node('receipt_0'),kind:'data'}));
 const input=object(record.sharedState.input_0);expect(await f.pipeline.read(input.inputRef as unknown as import('../src/contracts.js').ObservationRef)).toMatchObject({schemaRef:RUN_CONTRIBUTION_SCHEMA,payload:{value:20}});
 const receipt=object(record.sharedState.receipt_0);expect(await f.pipeline.read(receipt.inputRef as unknown as import('../src/contracts.js').ObservationRef)).toMatchObject({schemaRef:RUN_RECEIPT_SCHEMA,payload:{rawDisposition:'not-retained'}});
 expect(readRequirementResults([record.sharedState.requirement_1])[0]).toMatchObject({state:'unresolved',reason:'not-collected',affects:[expect.stringContaining(':completeness')]});
 expect(object(object(saved.payload).statistics).support).toHaveLength(2);
 // Removing per-write input tracking must remove this precise edge.
 const changed=JSON.parse(JSON.stringify(record)) as RuntimeSnapshot;
 const writer=findLastWriter(changed.commitLog,'statistics')!;
 const write=writer.trace.find(t=>t.path==='statistics')!;Object.assign(write,{readKeys:[]});
 const untracked=createRecordGraph([{address:changed.logAddress!,commitLog:changed.commitLog,complete:true}]);
 const without=await untracked.parents(node('statistics'),context);
 expect(without.edges.some(edge=>edge.nodeId===node('input_0')&&edge.kind==='data')).toBe(false);
});

it('keeps saved old assessments unchanged when a new cutoff is assessed',async()=>{
 const f=await fixture();const old=await summary(f);
 const newer=await f.pipeline.derive({...f.request,idempotencyKey:'later-assessment',request:{knowledgeCutoff:'2026-10-20T12:06:00.000Z'}});
 expect(newer.commit.status).toBe('committed');expect(newer.commit.observationRefs![0]!.id).not.toBe(old.ref.id);
 expect(await f.pipeline.read(old.ref)).toEqual(old);
});
it('retains raw-expiry facts without pretending the source response is available',async()=>{
 const f=await fixture([0]);const inputs=await loaded(f);const receipt=inputs[0]!.observations[0]!;
 inputs[0]!.observations[0]=changedPayload(receipt,{...object(receipt.payload),rawDisposition:'expired'});
 const value=await apply(f,inputs);expect(value.observations[0]!.payload).toMatchObject({statistics:{count:1,mean:20},rawEvidence:'consult-each-run-receipt'});
 expect(f.traces.at(-1)!.sharedState.receipt_0).toMatchObject({rawDisposition:'expired'});
});
it('refuses a false complete receipt on a partial collection batch',async()=>{
 const f=await fixture([0]);const inputs=await loaded(f);inputs[0]={...inputs[0]!,batch:{...inputs[0]!.batch,outcome:'partial',coverage:{...inputs[0]!.batch.coverage,status:'partial'}}};
 await expect(apply(f,inputs)).rejects.toMatchObject({code:'RUN_WINDOW_INVALID'});
});

it('the maximum 64-slot partition fits comfortably for the declared synthetic profile',async()=>{
 const f=await fixture(Array.from({length:64},(_,i)=>i),'complete',320);const saved=await summary(f);
 expect(saved.payload).toMatchObject({statistics:{count:64},coverage:{expectedPolls:64,missingPolls:0}});
 const bytes=Buffer.byteLength(JSON.stringify(f.traces[0]));
 expect(bytes).toBeLessThan(6*1024*1024);console.info(`LOCAL SEED 64-slot native record: ${bytes} bytes`);
}, 30_000);
it('a failed native trace save does not erase the summary inputs or numbers',async()=>{
 const f=await fixture([0]);const inputs=await loaded(f);
 const transform=createRunWindowEvidenceTransform({...f.options,enabled:true,traceStore:{async store(){throw Error('private-secret-marker');}}})!;
 const value=await transform.apply(inputs,f.request,{signal:new AbortController().signal,operationId:'trace-failure',startedAt:end,now:()=>end});
 expect(value.observations[0]!.payload).toMatchObject({statistics:{mean:20,count:1}});
 expect(value.details?.nativeTrace).toMatchObject({status:'unavailable'});expect(JSON.stringify(value)).not.toContain('private-secret-marker');
});
it('cancellation before evaluation publishes no native trace',async()=>{
 const f=await fixture([0]);const inputs=await loaded(f);const controller=new AbortController();controller.abort();
 await expect(f.transform.apply(inputs,f.request,{signal:controller.signal,operationId:'cancelled',startedAt:end,now:()=>end})).rejects.toMatchObject({code:'RUN_WINDOW_INVALID'});
 expect(f.traces).toHaveLength(0);
});

it('names an invalid saved contribution as invalid data without hiding other poll values',async()=>{
 const f=await fixture();const inputs=await loaded(f);const sample=inputs[0]!.observations[1]!;
 const payload=object(sample.payload);const {value:_value,...rest}=payload;
 inputs[0]!.observations[1]=changedPayload(sample,{...rest,status:'invalid'});
 const result=await apply(f,inputs);expect(result.observations[0]!.payload).toMatchObject({statistics:{count:1,mean:40,invalidCount:1}});
 expect((object(result.observations[0]!.payload).gaps as JsonValue[]).some(g=>object(g).reason==='invalid-data')).toBe(true);
});
