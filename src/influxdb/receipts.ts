import type { InfluxBatchReceipt, InfluxWriteCode, InfluxWriteReceipt } from './contracts.js';

function receiptStatus(total: number, acknowledged: number, uncertain: number, code?: InfluxWriteCode): InfluxWriteReceipt['status'] {
  if (total === 0 && !code) return 'empty';
  if (acknowledged === total && !code) return 'acknowledged';
  if (acknowledged > 0) return 'partial';
  return uncertain > 0 ? 'unknown' : 'not-written';
}

export function writeReceipt(totalPoints: number, batches: readonly InfluxBatchReceipt[], code?: InfluxWriteCode,
  rejectedPointIndex?: number): InfluxWriteReceipt {
  const acknowledgedPoints = batches.filter(batch => batch.status === 'acknowledged')
    .reduce((count, batch) => count + batch.pointCount, 0);
  const uncertainPoints = batches.filter(batch => batch.status === 'unknown')
    .reduce((count, batch) => count + batch.pointCount, 0);
  const notAttemptedPoints = totalPoints - acknowledgedPoints - uncertainPoints;
  const status = receiptStatus(totalPoints, acknowledgedPoints, uncertainPoints, code);
  return Object.freeze({ status, totalPoints, acknowledgedPoints, uncertainPoints, notAttemptedPoints,
    batches: Object.freeze(batches.map(batch => Object.freeze({ ...batch }))), ...(code ? { code } : {}),
    ...(rejectedPointIndex === undefined ? {} : { rejectedPointIndex }) });
}
