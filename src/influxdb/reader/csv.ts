import { ReadFault } from './contracts.js';

/** Bounded CSV text; consume records as they finish, without retaining a second row matrix. */
export function parseInfluxCsv(text: string, maxRows: number): readonly Readonly<Record<string, string>>[] {
  let row: string[] = [], cell = '', quoted = false, closed = false;
  let header: string[] | undefined, defaults: string[] = [];
  const result: Readonly<Record<string, string>>[] = [];
  function consume(cells: string[]) {
    if (cells.length === 1 && cells[0] === '') { header = undefined; defaults = []; return; }
    if (cells[0]?.startsWith('#')) {
      if (cells[0] === '#default') defaults = cells;
      header = undefined; return;
    }
    if (!header) {
      const valid = cells[0] === '' && cells[1] === 'result' && cells[2] === 'table';
      if (!valid || new Set(cells).size !== cells.length || cells.some(key => key.length > 1024)) throw new ReadFault('invalid-response');
      header = cells; return;
    }
    if (cells.length !== header.length) throw new ReadFault('invalid-response');
    const value: Record<string, string> = Object.create(null);
    for (let index = 1; index < header.length; index++) value[header[index]!] = cells[index] || defaults[index] || '';
    result.push(Object.freeze(value));
    if (result.length > maxRows) throw new ReadFault('limit-exceeded');
  }
  function endCell() {
    if (row.length >= 128 || cell.length > 65536) throw new ReadFault('limit-exceeded');
    row.push(cell); cell = ''; closed = false;
  }
  function endRow() { endCell(); consume(row); row = []; }
  for (let index = 0; index < text.length; index++) {
    const char = text[index]!;
    if (quoted) {
      if (char === '"') {
        if (text[index + 1] === '"') { cell += '"'; index++; }
        else { quoted = false; closed = true; }
      } else cell += char;
    } else if (char === ',') endCell();
    else if (char === '\n' || char === '\r') {
      if (char === '\r' && text[index + 1] === '\n') index++;
      endRow();
    } else if (char === '"' && !cell && !closed) quoted = true;
    else {
      if (closed || char === '"' || char === '\0') throw new ReadFault('invalid-response');
      cell += char;
    }
    if (cell.length > 65536) throw new ReadFault('limit-exceeded');
  }
  if (quoted) throw new ReadFault('invalid-response');
  if (cell || row.length || closed) endRow();
  return Object.freeze(result);
}
