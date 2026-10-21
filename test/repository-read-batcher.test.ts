import { expect, it } from 'vitest';
import { RepositoryReadBatcher } from '../src/storage/retainedRepository/readBatcher.js';
import { RepositorySerial, MAX_RETAINED_REQUESTS } from '../src/storage/retainedRepository/serial.js';
import { RepositoryRetentionError } from '../src/storage/retainedRepository/contracts.js';
import { candidate } from './helpers/filesystem-candidate.js';
it('batches same-turn reads in bounded chunks and never caches a completed result', async () => {
  const sizes: number[] = [];
  const batcher = new RepositoryReadBatcher(async refs => { sizes.push(refs.length); return refs.map(()=>undefined); });
  await Promise.all(Array.from({length:300},(_,i)=>batcher.read({kind:'observation',id:String(i)})));
  expect(sizes).toEqual([256,44]);
  await batcher.read({kind:'observation',id:'0'}); expect(sizes).toEqual([256,44,1]);
});
it('an expired reference does not make an unrelated fresh claim expire', async () => {
  const row = candidate().observations[0]!;
  const batcher = new RepositoryReadBatcher(async refs => {
    if(refs.some(ref=>ref.id==='expired')) throw new RepositoryRetentionError('REPOSITORY_EXPIRED');
    return refs.map(()=>row);
  });
  const values = await Promise.allSettled([batcher.read({kind:'observation',id:'expired'}),batcher.read(row.ref)]);
  expect(values[0]).toMatchObject({status:'rejected',reason:{code:'REPOSITORY_EXPIRED'}});
  expect(values[1]).toEqual({status:'fulfilled',value:row});
});
it('refuses excess queued reads instead of silently dropping them', async () => {
  const batcher = new RepositoryReadBatcher(async refs=>refs.map(()=>undefined));
  const pending=Array.from({length:8192},(_,i)=>batcher.read({kind:'observation',id:String(i)}));
  await expect(batcher.read({kind:'observation',id:'overflow'})).rejects.toMatchObject({code:'REPOSITORY_BUSY'});
  expect((await Promise.all(pending)).length).toBe(8192);
});
it('bounds queued transactions and drains in order after a refused excess request', async () => {
  const serial = new RepositorySerial(); let release!:()=>void;
  const gate = new Promise<void>(resolve=>{release=resolve;});
  const pending=Array.from({length:MAX_RETAINED_REQUESTS},(_,i)=>serial.run(async()=>{await gate;return i;}));
  await expect(serial.run(async()=>0)).rejects.toMatchObject({code:'REPOSITORY_BUSY'});
  release(); expect(await Promise.all(pending)).toEqual(Array.from({length:MAX_RETAINED_REQUESTS},(_,i)=>i));
  expect(serial.pending).toBe(0);
});
