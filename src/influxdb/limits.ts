import { INFLUX_DEFAULT_LIMITS, InfluxValidationError, type InfluxLimits } from './contracts.js';

type MutableLimits = { -readonly [Key in keyof Required<InfluxLimits>]: number };

export function normalizeInfluxLimits(limits?: InfluxLimits): Required<InfluxLimits> {
  try {
    if (limits === undefined) return Object.freeze({ ...INFLUX_DEFAULT_LIMITS });
    if (limits === null || typeof limits !== 'object' || Array.isArray(limits)) throw new Error();
    const prototype = Object.getPrototypeOf(limits);
    if (prototype !== Object.prototype && prototype !== null) throw new Error();
    const result: MutableLimits = { ...INFLUX_DEFAULT_LIMITS };
    for (const key of Reflect.ownKeys(limits)) {
      if (typeof key !== 'string' || !Object.hasOwn(result, key)) throw new Error();
      const property = Object.getOwnPropertyDescriptor(limits, key);
      if (!property || !property.enumerable || !('value' in property)) throw new Error();
      const name = key as keyof InfluxLimits;
      const value: unknown = property.value;
      if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0 || value > INFLUX_DEFAULT_LIMITS[name]) {
        throw new Error();
      }
      result[name] = value;
    }
    for (const [batch, total] of [['maxBatchPoints', 'maxPoints'], ['maxBatchBytes', 'maxBytes']] as const) {
      if (Object.hasOwn(limits, batch) && result[batch] > result[total]) throw new Error();
      result[batch] = Math.min(result[batch], result[total]);
    }
    return Object.freeze(result);
  } catch {
    throw new InfluxValidationError('invalid-options');
  }
}
