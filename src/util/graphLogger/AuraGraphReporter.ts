import neo4j, { Driver } from 'neo4j-driver';
import { flattenProps } from './flattenProps';

export type AuraConfig = {
  uri: string;
  database: string;
  user: string;
  password: string;
  batchSize?: number;
  flushIntervalMs?: number;
  maxBatchSize?: number;
  maxRetries?: number;
};

export type AuraLogEvent = {
  signature: string;
  parentSignature?: string;
  edgeType: string;
  bizProps: Record<string, any>;
  resultProps: Record<string, any>;
  nodeProps: Record<string, any>;
};

let _instance: AuraGraphReporter | undefined;

class AuraGraphReporter {
  private config: Required<Pick<AuraConfig, 'batchSize' | 'flushIntervalMs' | 'maxBatchSize' | 'maxRetries'>> & AuraConfig;
  private driver?: Driver;
  private eventQueue: AuraLogEvent[] = [];
  private flushTimer?: ReturnType<typeof setInterval>;
  private droppedEvents = 0;
  private sentEvents = 0;

  constructor(config: AuraConfig) {
    this.config = {
      batchSize: 500,
      flushIntervalMs: 1_000,
      maxBatchSize: 2_000,
      maxRetries: 6,
      ...config,
    };
    if (this.isEnabled()) {
      this.ensureDriver();
      this.startFlushLoop();
      console.log(
        `[AuraGraphReporter] started uri=${this.obfuscatedUri()} db=${this.config.database} batch=${this.config.batchSize}`,
      );
    } else {
      console.log('[AuraGraphReporter] disabled: missing config');
    }
  }

  // ── Singleton ──────────────────────────────────────────────
  static init(config: AuraConfig) {
    _instance?.stop();
    _instance = new AuraGraphReporter(config);
    return _instance;
  }

  static get instance(): AuraGraphReporter | undefined {
    return _instance;
  }

  // ── High-level helpers (called from callApi interceptor) ──

  /** Fire-and-forget: enqueue an API call start event */
  static logTaskEnqueued(taskName: string, argsFlat: Record<string, any>) {
    if (!_instance?.isEnabled()) return;

    const sig = `${taskName}::${Date.now()}::${Math.random().toString(36).slice(2, 8)}`;

    _instance.enqueueEvent({
      signature: sig,
      edgeType: 'apiCall',
      bizProps: { method: taskName, ...argsFlat },
      resultProps: {},
      nodeProps: {
        firstName: taskName,
        tsMs: Date.now(),
      },
    });

    return sig;
  }

  /** Fire-and-forget: enqueue an API call success event */
  static logTaskSuccess(
    taskName: string,
    signature: string,
    argsFlat: Record<string, any>,
    resultFlat: Record<string, any>,
    durationMs: number,
  ) {
    if (!_instance?.isEnabled()) return;

    _instance.enqueueEvent({
      signature,
      edgeType: 'apiCall',
      bizProps: { method: taskName, durationMs, ...argsFlat },
      resultProps: { ...resultFlat, durationMs },
      nodeProps: {
        firstName: taskName,
        outcome: 'ok',
        durationMs,
        tsMs: Date.now(),
      },
    });
  }

  /** Fire-and-forget: enqueue an API call error event */
  static logTaskError(
    taskName: string,
    signature: string,
    argsFlat: Record<string, any>,
    errorMessage: string,
  ) {
    if (!_instance?.isEnabled()) return;

    _instance.enqueueEvent({
      signature,
      edgeType: 'apiCall',
      bizProps: { method: taskName, ...argsFlat },
      resultProps: { error: errorMessage },
      nodeProps: {
        firstName: taskName,
        outcome: errorMessage,
        tsMs: Date.now(),
      },
    });
  }

  // ── Core ───────────────────────────────────────────────────

  isEnabled() {
    return !!this.config.uri && !!this.config.user && !!this.config.password;
  }

  enqueueEvent(event: AuraLogEvent) {
    if (!this.isEnabled()) return;
    this.eventQueue.push(event);
  }

  private startFlushLoop() {
    this.flushTimer = setInterval(() => {
      void this.flushBuffer();
    }, this.config.flushIntervalMs);
  }

  async flushBuffer() {
    if (!this.isEnabled() || this.eventQueue.length === 0) return;
    const buffer = this.eventQueue.splice(0, this.config.batchSize);
    let sentInThisFlush = 0;
    const chunks = this.chunkArray(buffer, this.config.maxBatchSize);
    for (const chunk of chunks) {
      const ok = await this.sendChunkWithRetry(chunk);
      if (!ok) {
        this.droppedEvents += chunk.length;
        continue;
      }
      sentInThisFlush += chunk.length;
    }
    this.sentEvents += sentInThisFlush;
    if (sentInThisFlush > 0) {
      console.log(`[AuraGraphReporter] flush ok events=${sentInThisFlush} totalSent=${this.sentEvents} dropped=${this.droppedEvents}`);
    }
  }

  private chunkArray<T>(arr: T[], size: number): T[][] {
    const res: T[][] = [];
    for (let i = 0; i < arr.length; i += size) {
      res.push(arr.slice(i, i + size));
    }
    return res;
  }

  private async sendChunkWithRetry(chunk: AuraLogEvent[]): Promise<boolean> {
    let attempt = 0;
    while (attempt <= this.config.maxRetries) {
      try {
        const ok = await this.postChunk(chunk);
        if (ok) return true;
      } catch (e) {
        console.warn(`[AuraGraphReporter] batch send failed attempt=${attempt}`, e);
      }
      await this.delay(this.backoffMs(attempt));
      attempt++;
    }
    return false;
  }

  private async postChunk(chunk: AuraLogEvent[]): Promise<boolean> {
    const driver = this.ensureDriver();
    if (!driver) return false;

    const session = driver.session({ database: this.config.database });
    const rows = chunk.map((event) => ({
      signature: event.signature,
      parentSignature: event.parentSignature ?? '',
      bizProps: event.bizProps,
      resultProps: event.resultProps,
      nodeProps: event.nodeProps,
    }));

    const statement = `
      UNWIND $rows AS row
      MERGE (t {signature: row.signature})
      SET t += row.nodeProps,
          t.resultType = row.resultProps.resultType
      FOREACH (_ IN CASE WHEN row.parentSignature IS NULL OR row.parentSignature = '' THEN [] ELSE [1] END |
        MERGE (p {signature: row.parentSignature})
        MERGE (p)-[rel:next]->(t)
        SET rel += row.bizProps
        FOREACH (_ IN CASE WHEN row.resultProps.resultType IS NOT NULL THEN [1] ELSE [] END |
          MERGE (t)-[resp:response]->(p)
          SET resp += row.resultProps
        )
      )
    `;

    try {
      const tx = session.beginTransaction();
      await tx.run(statement, { rows });
      await tx.commit();
      await session.close();
      return true;
    } catch (e) {
      console.warn('[AuraGraphReporter] Bolt write failed', e);
      await session.close();
      return false;
    }
  }

  private ensureDriver(): Driver | undefined {
    if (this.driver) return this.driver;
    try {
      this.driver = neo4j.driver(
        this.config.uri,
        neo4j.auth.basic(this.config.user, this.config.password),
      );
      return this.driver;
    } catch (e) {
      console.warn('[AuraGraphReporter] driver init failed', e);
      return undefined;
    }
  }

  private delay(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private backoffMs(attempt: number): number {
    const base = 200;
    const max = 10_000;
    return Math.min(base * (1 << Math.min(attempt, 6)), max);
  }

  private obfuscatedUri(): string {
    const host = this.config.uri || '';
    if (host.length <= 6) return host;
    return `${host.slice(0, 3)}***${host.slice(-3)}`;
  }

  stop() {
    if (this.flushTimer) clearInterval(this.flushTimer);
    this.driver?.close();
    this.driver = undefined;
    _instance = undefined;
    console.log(`[AuraGraphReporter] stopped. totalSent=${this.sentEvents} totalDropped=${this.droppedEvents}`);
  }
}

export default AuraGraphReporter;
