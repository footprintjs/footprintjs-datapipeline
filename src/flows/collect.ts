import { flowChart } from 'footprintjs';
import type { CandidateBatch, CollectRequest, CommitReceipt, Source } from '../contracts.js';
import type { CommitWrite } from '../ports.js';

export interface CollectState {
  pipelineRequest?: CollectRequest;
  source?: Source;
  candidate?: CandidateBatch;
  write?: CommitWrite;
  receipt?: CommitReceipt;
}
export interface CollectSteps {
  source: Source;
  acquire(request: CollectRequest): Promise<CandidateBatch>;
  prepare(candidate: CandidateBatch): CommitWrite;
  publish(write: CommitWrite): Promise<CommitReceipt>;
}

/** Each stage uses normal tracked Footprint scope reads and writes. */
export function collectFlow(steps: CollectSteps) {
  return flowChart<CollectState>('Resolve source', scope => {
    scope.pipelineRequest = scope.$getArgs<{ invocation: CollectRequest }>().invocation;
    scope.source = steps.source;
  }, 'resolve-source')
    .addFunction('Collect observations', async scope => {
      scope.candidate = await steps.acquire(scope.pipelineRequest!);
    }, 'collect-observations')
    .addFunction('Prepare records and evidence', scope => {
      scope.write = steps.prepare(scope.candidate!);
    }, 'prepare-collection')
    .addFunction('Publish records and evidence', async scope => {
      scope.receipt = await steps.publish(scope.write!);
    }, 'publish-collection')
    .build();
}
