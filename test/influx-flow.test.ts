import { expect, test } from 'vitest';
import { decide, flowChart } from 'footprintjs';
import { flattenCausalDAG, keysReadFromExecutionTree, sliceForKey } from 'foottrace';
import { runWindowFlow } from '../src/index.js';
import { createInfluxDbSink, type InfluxPoint, type InfluxWriteReceipt } from '../src/influxdb/index.js';

interface State {
  source: { value: number; rowRef: string };
  point: InfluxPoint;
  unrelated: InfluxPoint;
  delivery: InfluxWriteReceipt;
  status: string;
  disposition: string;
  unrelatedMeasurement: string;
}

for (const httpStatus of [204, 400]) {
  test(`an ordinary Footprint publish step records ${httpStatus} delivery without changing source evidence`, async () => {
    const credential = 'SYNTHETIC_CREDENTIAL_MUST_NOT_ENTER_THE_RECORD';
    const sink = createInfluxDbSink({ url: 'http://127.0.0.1:8086', org: 'test', bucket: 'shadow',
      getToken: () => credential, fetch: async () => new Response(null, { status: httpStatus }) });
    const chart = flowChart<State>('Retain source sample and receipt', scope => {
      scope.source = { value: 5000, rowRef: 'fixture:saved-response#/rows/0' };
    }, 'source')
      .addFunction('Map this source sample to one typed point', scope => {
        const sample = scope.source;
        scope.point = { measurement: 'synthetic_metric', tags: [],
          fields: [['value', { type: 'float', value: sample.value }]], timestampNs: '1792411200125000000' };
      }, 'point')
      .addFunction('Keep a second resource separate', scope => {
        scope.unrelated = { measurement: 'unrelated_metric', tags: [],
          fields: [['value', { type: 'float', value: 99 }]], timestampNs: '1792411200125000000' };
      }, 'unrelated')
      .addFunction('Publish this point and retain its delivery receipt', async scope => {
        // The sink reads the tracked point and snapshots it before awaiting.
        const point = scope.point;
        scope.delivery = await sink.write([point]);
        scope.status = scope.delivery.status;
        // A later unrelated read must not become evidence for the earlier write.
        scope.unrelatedMeasurement = scope.unrelated.measurement;
      }, 'publish')
      .addDeciderFunction('Classify the delivery acknowledgement', scope => decide(scope, [
        { when: { status: { eq: 'acknowledged' } }, then: 'sent', label: 'All points acknowledged by InfluxDB' },
      ], { branch: 'uncertain', label: 'Delivery has no complete acknowledgement' }), 'delivery-rule')
      .addFunctionBranch('sent', 'Record acknowledged delivery', scope => { scope.disposition = 'acknowledged'; })
      .addFunctionBranch('uncertain', 'Record uncertain delivery', scope => { scope.disposition = 'unknown'; })
      .setDefault('uncertain').end().build();
    const { record } = await runWindowFlow({ operationId: `fixture:influx-${httpStatus}`, chart });
    expect(record.sharedState.source).toEqual({ value: 5000, rowRef: 'fixture:saved-response#/rows/0' });
    expect(record.sharedState.delivery).toMatchObject({ status: httpStatus === 204 ? 'acknowledged' : 'unknown' });
    expect(record.sharedState.disposition).toBe(httpStatus === 204 ? 'acknowledged' : 'unknown');
    expect(JSON.stringify(record)).not.toContain(credential);
    const slice = sliceForKey(record.commitLog, 'delivery', keysReadFromExecutionTree(record.executionTree), {
      edgeAttribution: 'per-write',
    });
    expect(slice.root).toBeDefined();
    const stages = flattenCausalDAG(slice.root!).map(node => node.stageId);
    expect(stages).toEqual(expect.arrayContaining(['source', 'point', 'publish']));
    expect(stages).not.toContain('unrelated');
  });
}
