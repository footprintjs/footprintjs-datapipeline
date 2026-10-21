import { join } from 'node:path';
import type { RecordRef, StoredRecord } from '../../contracts.js';
import { FilesystemRepository } from '../filesystem.js';
import { refKey } from '../filesystemRecords.js';
/** Native packed decoder and content checks, with ledger-selected operation addresses. */
export class IndexedData extends FilesystemRepository {
  constructor(directory: string, private readonly lookup: (refs: readonly RecordRef[]) => Promise<readonly (StoredRecord | undefined)[]>) { super({ directory, packing: 'gzip' }); }
  override async read(ref: RecordRef): Promise<StoredRecord | undefined> { return (await this.lookup([ref]))[0]; }
  override readMany(refs: readonly RecordRef[]): Promise<readonly (StoredRecord | undefined)[]> { return this.lookup(refs); }
  async readAt(hash: string, refs: readonly RecordRef[]): Promise<readonly (StoredRecord | undefined)[]> {
    const directory = join(this.directory, 'operations', hash);
    const { manifest, packed } = await this.operation(directory);
    const entries = new Map(manifest.records.map(entry => [refKey(entry.ref), entry]));
    return Promise.all(refs.map(ref => { const entry = entries.get(refKey(ref)); return entry ? this.record(directory, entry, packed) : undefined; }));
  }
  async reconcileHash(hash: string) {
    try { const { manifest } = await this.operation(join(this.directory, 'operations', hash)); return await this.findOperation(manifest.operationId); }
    catch (error) { if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') return undefined; throw error; }
  }
}
