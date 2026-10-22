import { createRequire } from 'node:module';
import { expect, it } from 'vitest';
import { checkSchema, createSchema } from '../src/storage/sealedHistory/schema.js';

it('refuses an otherwise matching marker with a journal mode outside explicit DELETE/WAL support', () => {
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  const db = new DatabaseSync(':memory:');
  try {
    createSchema(db, 100);
    expect(db.prepare('PRAGMA journal_mode').get()?.journal_mode).toBe('memory');
    expect(() => checkSchema(db, 100)).toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_UNMANAGED' }));
  } finally { db.close(); }
});
