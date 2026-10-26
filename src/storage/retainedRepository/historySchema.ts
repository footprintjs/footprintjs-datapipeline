import type { DatabaseSync } from 'node:sqlite';
/** New namespace only. Counters, progress and ownership mutate in one transaction. */
export function createHistorySchema(db: DatabaseSync): void {
  db.exec(`CREATE TABLE IF NOT EXISTS retired_history(id INTEGER PRIMARY KEY CHECK(id=1),
    floor INTEGER NOT NULL,generation INTEGER NOT NULL,operations INTEGER NOT NULL,
    refs INTEGER NOT NULL,unconfirmed INTEGER NOT NULL,sha256 TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS history_counts(id INTEGER PRIMARY KEY CHECK(id=1),operations INTEGER NOT NULL CHECK(operations>=0),refs INTEGER NOT NULL CHECK(refs>=0));
    CREATE TABLE IF NOT EXISTS history_scan(id INTEGER PRIMARY KEY CHECK(id=1),floor INTEGER NOT NULL,period INTEGER NOT NULL,
      cursorOp TEXT NOT NULL,cursorRef TEXT NOT NULL,phase TEXT NOT NULL,operations INTEGER NOT NULL,refs INTEGER NOT NULL,
      unconfirmed INTEGER NOT NULL,sha256 TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS period_members ON operations(period,operationHash);
    CREATE INDEX IF NOT EXISTS reference_owners ON refs(operationHash,refHash);
    CREATE TRIGGER IF NOT EXISTS history_operation_insert AFTER INSERT ON operations BEGIN
      UPDATE history_counts SET operations=operations+1 WHERE id=1; END;
    CREATE TRIGGER IF NOT EXISTS history_operation_delete AFTER DELETE ON operations BEGIN
      UPDATE history_counts SET operations=operations-1 WHERE id=1; END;
    CREATE TRIGGER IF NOT EXISTS history_ref_insert AFTER INSERT ON refs BEGIN
      UPDATE history_counts SET refs=refs+1 WHERE id=1; END;
    CREATE TRIGGER IF NOT EXISTS history_ref_delete AFTER DELETE ON refs BEGIN
      UPDATE history_counts SET refs=refs-1 WHERE id=1; END;
    INSERT OR IGNORE INTO history_counts VALUES(1,0,0);`);
  db.prepare('INSERT OR IGNORE INTO retired_history VALUES(1,0,0,0,0,0,?)').run('0'.repeat(64));
  db.prepare("INSERT OR IGNORE INTO history_scan VALUES(1,0,-1,'','','operation',0,0,0,?)").run('0'.repeat(64));
}
