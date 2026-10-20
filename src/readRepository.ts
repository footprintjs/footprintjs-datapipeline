import type { RecordRef, StoredRecord } from './contracts.js';
import { MAX_REPOSITORY_READ_BATCH, type Repository } from './ports.js';
import { ensure } from './validation.js';

/** Bounded optional acceleration; ports without it retain individual reads. */
export async function readRepositoryRecords(repository: Repository, refs: readonly RecordRef[], signal?: AbortSignal): Promise<readonly (StoredRecord | undefined)[]> {
  const result: (StoredRecord | undefined)[] = [];
  for (let i = 0; i < refs.length; i += MAX_REPOSITORY_READ_BATCH) {
    signal?.throwIfAborted();
    const chunk = refs.slice(i, i + MAX_REPOSITORY_READ_BATCH);
    if (repository.readMany) {
      const values = await repository.readMany(chunk);
      ensure(Array.isArray(values) && values.length === chunk.length, 'Repository bulk read returned incomplete results');
      result.push(...values);
    } else {
      for (const ref of chunk) { signal?.throwIfAborted(); result.push(await repository.read(ref)); }
    }
  }
  return result;
}
