import { AttributeMap } from "./ScanQueryPipeline";

interface IteratorExecutor<T> {
  execute(): AsyncGenerator<T[], { lastEvaluatedKey: Record<string, unknown> } | void, void>;
}
export class TableIterator<T = AttributeMap, P = undefined> {
  private lastEvaluatedKeyHandlers: Array<(k: Record<string, unknown>) => void> = [];
  config: {
    parent: P;
    fetcher: IteratorExecutor<T>;
  };

  constructor(fetcher: IteratorExecutor<T>, parent?: P) {
    this.config = { parent: parent as P, fetcher };
  }

  async forEachStride(
    iterator: (items: T[], index: number, parent: P, cancel: () => void) => Promise<any> | void
  ): Promise<P> {
    let index = 0;

    await this.iterate(this.config.fetcher, async (stride, cancel) => {
      await iterator(stride, index, this.config.parent, cancel);
      index += 1;
    });

    return this.config.parent;
  }

  onLastEvaluatedKey(handler: (lastEvaluatedKey: Record<string, unknown>) => void): this {
    this.lastEvaluatedKeyHandlers.push(handler);
    return this;
  }

  private async iterate(
    fetcher: IteratorExecutor<T>,
    iterator: (stride: T[], cancel: () => void) => Promise<void>
  ): Promise<void> {
    let cancelled = false;
    const cancel = () => {
      cancelled = true;
    };
    const executor = fetcher.execute();

    while (true) {
      if (cancelled) {
        break;
      }
      const stride = await executor.next();
      const { value } = stride;
      if (stride.done) {
        this.handleDone(stride);
        break;
      }

      await iterator(value as T[], cancel);
    }
  }

  private handleDone(iteratorResponse: { done: true; value?: void | { lastEvaluatedKey: Record<string, unknown> } }) {
    const { value } = iteratorResponse;
    if (value && "lastEvaluatedKey" in value) {
      this.lastEvaluatedKeyHandlers.forEach((h) => h(value.lastEvaluatedKey));
      this.lastEvaluatedKeyHandlers = [];
    }
  }

  // when a promise is returned, all promises are resolved in the batch before processing the next batch
  async forEach(
    iterator: (item: T, index: number, pipeline: P, cancel: () => void) => Promise<any> | void
  ): Promise<P> {
    let index = 0;
    let iteratorPromises: unknown[] = [];
    let cancelled = false;
    const cancelForEach = () => {
      cancelled = true;
    };

    await this.iterate(this.config.fetcher, async (stride, cancel) => {
      iteratorPromises = [];
      for (const item of stride) {
        const iteratorResponse = iterator(item, index, this.config.parent, cancelForEach);
        index += 1;

        if (cancelled) {
          await Promise.all(iteratorPromises);
          cancel();
          break;
        } else if (typeof iteratorResponse === "object" && iteratorResponse instanceof Promise) {
          iteratorPromises.push(iteratorResponse);
        }
      }

      await Promise.all(iteratorPromises);
    });

    await Promise.all(iteratorPromises);

    return this.config.parent;
  }

  async map<U>(iterator: (item: T, index: number) => U): Promise<U[]> {
    const results: U[] = [];

    let index = 0;

    await this.iterate(this.config.fetcher, (stride, _cancel) => {
      for (const item of stride) {
        results.push(iterator(item, index));
        index += 1;
      }

      return Promise.resolve();
    });
    return results;
  }

  filterLazy(predicate: (item: T, index: number) => boolean): TableIterator<T, P> {
    const existingFetcher = this.config.fetcher;

    let index = 0;
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const that = this;

    const fetcher = async function* () {
      const executor = existingFetcher.execute();
      while (true) {
        const stride = await executor.next();

        if (stride.done) {
          that.handleDone(stride);
          break;
        }

        yield stride.value.filter((val, i) => {
          const filtered = predicate(val, index);
          index += 1;
          return filtered;
        });
      }
    };

    return new TableIterator({ execute: fetcher }, this.config.parent);
  }

  mapLazy<U>(iterator: (item: T, index: number) => U): TableIterator<U, P> {
    const existingFetcher = this.config.fetcher;
    let results: U[] = [];
    let index = 0;
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const that = this;

    const fetcher = async function* () {
      const executor = existingFetcher.execute();

      while (true) {
        const stride = await executor.next();

        if (stride.done) {
          that.handleDone(stride);
          break;
        }

        results = stride.value.map((item) => {
          const result = iterator(item, index);
          index += 1;
          return result;
        });

        yield results;
      }
    };

    return new TableIterator({ execute: fetcher }, this.config.parent);
  }

  all(): Promise<T[]> {
    const result = this.map((i) => i);
    return result;
  }

  async *iterator(): AsyncGenerator<T, void, void> {
    const executor = this.config.fetcher.execute();

    while (true) {
      const stride = await executor.next();
      if (stride.done) {
        this.handleDone(stride);
        return;
      }

      for (const item of stride.value) {
        yield item;
      }
    }
  }

  strideIterator(): AsyncGenerator<T[], Record<string, unknown> | void, void> {
    return this.config.fetcher.execute();
  }
}
