import { expect, it, vi } from 'vitest';
import { readRepositoryRecords } from '../src/readRepository.js';
import type { Repository } from '../src/ports.js';
const refs=Array.from({length:600},(_,i)=>({kind:'observation' as const,id:String(i)}));
function port(): Repository {return {async read(){return undefined;},async findOperation(){return undefined;},async commit(){throw Error('No write in read test');}};}
it('uses bounded bulk requests with exact order and length',async()=>{
 const repository=port();const read=vi.spyOn(repository,'read');const sizes:number[]=[];
 repository.readMany=async batch=>{sizes.push(batch.length);return batch.map(()=>undefined);};
 expect(await readRepositoryRecords(repository,refs)).toHaveLength(600);expect(sizes).toEqual([256,256,88]);expect(read).not.toHaveBeenCalled();
});
it('keeps scalar-only ports working without adding required capabilities',async()=>{
 const repository=port();const read=vi.spyOn(repository,'read');
 await readRepositoryRecords(repository,refs.slice(0,3));expect(read.mock.calls.map(([ref])=>ref.id)).toEqual(['0','1','2']);
});
it('rejects truncated bulk replies rather than silently losing observations',async()=>{
 const repository=port();repository.readMany=async()=>[];
 await expect(readRepositoryRecords(repository,refs.slice(0,2))).rejects.toThrow('Repository bulk read returned incomplete results');
});
it('schedules no next batch after cancellation',async()=>{
 const controller=new AbortController(),repository=port();let calls=0;
 repository.readMany=async batch=>{calls++;controller.abort();return batch.map(()=>undefined);};
 await expect(readRepositoryRecords(repository,refs,controller.signal)).rejects.toThrow();expect(calls).toBe(1);
});
