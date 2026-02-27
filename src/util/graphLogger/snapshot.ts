export function snapshotOf(result: any, maxDepth = 4, currentDepth = 0): Record<string, any> {
    if (result == null) return {};

    const map: Record<string, any> = {};

    // For the top level result type
    if (currentDepth === 0) {
        map['resultType'] = result?.constructor?.name || typeof result;
    }

    if (currentDepth >= maxDepth) return map;

    function extractNode(obj: any, prefix: string, depth: number) {
        if (obj == null || depth >= maxDepth) return;

        // Skip cyclic references or complex structures (e.g., DOM nodes)
        if (typeof window !== 'undefined' && (obj instanceof Element || obj instanceof Window || obj instanceof document.constructor)) {
            return;
        }

        const keys = Object.keys(obj);
        for (const key of keys) {
            // Ignore synthetic or private-looking fields
            if (key.startsWith('$') || key.startsWith('_')) continue;

            const value = obj[key];
            if (value == null) continue;

            const mapKey = prefix ? `${prefix}_${key}` : key;

            if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
                map[mapKey] = value;
            } else if (Array.isArray(value) || ArrayBuffer.isView(value)) {
                // TypedArrays map to ArrayBuffer.isView
                const arr = value as any[];
                map[`${mapKey}Count`] = arr.length;
                if (arr.length === 1 && arr[0] != null) {
                    if (typeof arr[0] === 'string' || typeof arr[0] === 'number' || typeof arr[0] === 'boolean') {
                        map[`${mapKey}Item`] = arr[0];
                    } else if (typeof arr[0] === 'object') {
                         extractNode(arr[0], `${mapKey}Item`, depth + 1);
                    }
                }
            } else if (typeof value === 'object') {
                // Check if it's a domain object vs plain object
                const isDomainObj = value.constructor && value.constructor.name !== 'Object' && value.constructor.name !== 'Array';
                
                // Specific fields we might care about for Telegram data objects
                if (isDomainObj || key.toLowerCase().includes('message') || key.toLowerCase().includes('chat') || key.toLowerCase().includes('user')) {
                    const criticalProps = ['id', 'chatId', 'userId', 'messageId', 'date', 'feedId'];
                    let foundCritical = false;
                    for (const propName of criticalProps) {
                        const propValue = value[propName];
                        if (typeof propValue === 'number' || typeof propValue === 'string') {
                            map[`${mapKey}_${propName}`] = propValue;
                            foundCritical = true;
                        }
                    }
                    
                    const arrayProps = ['messages', 'chatIds'];
                    for (const propName of arrayProps) {
                        const propValue = value[propName];
                        if (Array.isArray(propValue) || ArrayBuffer.isView(propValue)) {
                           const arr = propValue as any[];
                           map[`${mapKey}_${propName}Count`] = arr.length;
                           if (arr.length === 1 && arr[0] != null) {
                               if (typeof arr[0] === 'string' || typeof arr[0] === 'number' || typeof arr[0] === 'boolean') {
                                   map[`${mapKey}_${propName}Item`] = arr[0];
                               } else {
                                   extractNode(arr[0], `${mapKey}_${propName}Item`, depth + 1);
                               }
                           }
                           foundCritical = true;
                        }
                    }
                    
                    if (!foundCritical && depth + 1 < maxDepth) {
                        extractNode(value, mapKey, depth + 1);
                    }
                } else if (depth + 1 < maxDepth) {
                    // Deep traverse plain objects if within depth limit
                    extractNode(value, mapKey, depth + 1);
                }
            }
        }
    }

    try {
        if (typeof result === 'object' && !Array.isArray(result)) {
            extractNode(result, '', currentDepth);
        } else if (Array.isArray(result)) {
            map['resultCount'] = result.length;
            if (result.length === 1) {
                if (typeof result[0] === 'object') {
                    extractNode(result[0], 'resultItem', currentDepth);
                } else {
                    map['resultItem'] = result[0];
                }
            }
        } else {
            map['resultValue'] = result;
        }
    } catch (e) {
        console.warn('snapshotOf failed to extract fields', e);
    }

    return map;
}
