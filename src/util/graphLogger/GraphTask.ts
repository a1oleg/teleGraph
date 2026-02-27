import AuraGraphReporter from './AuraGraphReporter';
import { flattenProps } from './flattenProps';

export function GraphTask(taskName: string) {
  return function (target: any, propertyKey: string, descriptor: PropertyDescriptor) {
    const originalMethod = descriptor.value;

    descriptor.value = async function (...args: any[]) {
      // 1. Создаем событие "Начало"
      AuraGraphReporter.logTaskEnqueued?.(taskName, args);
      const startTime = Date.now();
      try {
        const result = await originalMethod.apply(this, args);
        // 2. Успех: собираем snapshot и отправляем
        AuraGraphReporter.logTaskSuccess?.(
          taskName,
          flattenProps(args),
          flattenProps(result),
          Date.now() - startTime
        );
        return result;
      } catch (error: any) {
        // 3. Ошибка: отправляем в граф "Провал"
        AuraGraphReporter.logTaskError?.(
          taskName,
          flattenProps(args),
          error?.message || String(error)
        );
        throw error;
      }
    };
    return descriptor;
  };
}
