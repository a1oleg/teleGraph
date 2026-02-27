import neo4j, { Driver, Session } from 'neo4j-driver';

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

class AuraGraphReporter {
  private config: AuraConfig;
  private driver?: Driver;
  private eventQueue: AuraLogEvent[] = [];
  private flushTimer?: NodeJS.Timeout;
  private droppedEvents = 0;
  private sentEvents = 0;

  constructor(config: AuraConfig) {
    this.config = {
      batchSize: 500,
      flushIntervalMs: 1000,
      maxBatchSize: 2000,
      maxRetries: 6,
      ...config,
    };
    if (this.isEnabled()) {
      this.ensureDriver();
      this.startFlushLoop();
    }
  }

  isEnabled() {
    return (
      !!this.config.uri && !!this.config.user && !!this.config.password
    );
  }

  enqueueEvent(event: AuraLogEvent) {
    if (!this.isEnabled()) return;
    this.eventQueue.push(event);
  }

  private startFlushLoop() {
    this.flushTimer = setInterval(() => {
      this.flushBuffer();
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
        // log error if needed
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
    const rows = chunk.map(event => ({
      signature: event.signature,
      parentSignature: event.parentSignature,
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
      await session.writeTransaction(tx => tx.run(statement, { rows }));
      await session.close();
      return true;
    } catch (e) {
      await session.close();
      return false;
    }
  }

  private ensureDriver(): Driver | undefined {
    if (this.driver) return this.driver;
    try {
      this.driver = neo4j.driver(
        this.config.uri,
        neo4j.auth.basic(this.config.user, this.config.password)
      );
      return this.driver;
    } catch (e) {
      return undefined;
    }
  }

  private delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private backoffMs(attempt: number): number {
    const base = 200;
    const max = 10000;
    return Math.min(base * (1 << Math.min(attempt, 6)), max);
  }

  stop() {
    if (this.flushTimer) clearInterval(this.flushTimer);
    this.driver?.close();
    this.driver = undefined;
  }
}

export default AuraGraphReporter;
