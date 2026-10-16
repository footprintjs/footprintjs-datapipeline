import { flowChart } from 'footprintjs';
import type { BatchInput, CandidateBatch, CommitReceipt, DeriveRequest } from '../contracts.js';
import type { CommitWrite } from '../ports.js';

export interface DeriveState {
  pipelineRequest?: DeriveRequest;
  inputs?: readonly BatchInput[];
  candidate?: CandidateBatch;
  write?: CommitWrite;
  receipt?: CommitReceipt;
}
export interface DeriveSteps {
  load(request: DeriveRequest): Promise<readonly BatchInput[]>;
  apply(inputs: readonly BatchInput[], request: DeriveRequest): Promise<CandidateBatch>;
  prepare(candidate: CandidateBatch): CommitWrite;
  publish(write: CommitWrite): Promise<CommitReceipt>;
}
export function deriveFlow(steps: DeriveSteps) {
  return flowChart<DeriveState>('Resolve committed inputs', async scope => {
    scope.pipelineRequest = scope.$getArgs<{ invocation: DeriveRequest }>().invocation;
    scope.inputs = await steps.load(scope.pipelineRequest);
  }, 'resolve-inputs')
    .addFunction('Derive observations', async scope => {
      scope.candidate = await steps.apply(scope.inputs!, scope.pipelineRequest!);
    }, 'derive-observations')
    .addFunction('Prepare records and evidence', scope => {
      scope.write = steps.prepare(scope.candidate!);
    }, 'prepare-derivation')
    .addFunction('Publish records and evidence', async scope => {
      scope.receipt = await steps.publish(scope.write!);
    }, 'publish-derivation')
    .build();
}
