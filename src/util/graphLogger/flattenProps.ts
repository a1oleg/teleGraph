// Utility to flatten complex objects for AuraGraphReporter
// Cycles, DOM elements, and large arrays are skipped or reduced

export function flattenProps(obj: any, opts?: {
  maxArrayLength?: number;
  seen?: Set<any>;
  prefix?: string;
}): Record<string, any> {
  const result: Record<string, any> = {};
  const seen = opts?.seen || new Set();
  const maxArrayLength = opts?.maxArrayLength ?? 10;
  const prefix = opts?.prefix ?? '';

  function add(key: string, value: any) {
    result[prefix ? `${prefix}_${key}` : key] = value;
  }

  function walk(val: any, key: string) {
    if (val == null) return;
    if (seen.has(val)) return;
    if (typeof val === 'object') {
      seen.add(val);
      // Skip DOM elements
      if (typeof window !== 'undefined' && val instanceof window.Element) return;
      // MTProto/Telegram: try to extract IDs and types
      if (val.constructor && val.constructor.name && /^Api/.test(val.constructor.name)) {
        if ('id' in val) add(key + '_id', val.id);
        if ('userId' in val) add(key + '_userId', val.userId);
        if ('chatId' in val) add(key + '_chatId', val.chatId);
        if ('messageId' in val) add(key + '_messageId', val.messageId);
        add(key + '_type', val.constructor.name);
        return;
      }
      // Arrays
      if (Array.isArray(val)) {
        add(key + 'Count', val.length);
        if (val.length > 0 && val.length <= maxArrayLength) {
          val.forEach((item, idx) => walk(item, key + 'Item' + idx));
        }
        return;
      }
      // Other objects
      for (const prop in val) {
        if (!Object.prototype.hasOwnProperty.call(val, prop)) continue;
        walk(val[prop], key ? key + '_' + prop : prop);
      }
      return;
    }
    // Primitives
    if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') {
      add(key, val);
    } else if (typeof val === 'function') {
      // skip
    }
  }

  walk(obj, '');
  return result;
}
