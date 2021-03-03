import DynamoDB from "aws-sdk/clients/dynamodb";

interface IteratorExecutor<T> {
  execute(): AsyncGenerator<T[], void, void>;
}
export class TableIterator<T = DynamoDB.AttributeMap, P = undefined> {
  config: {
    pipeline: P;
    fetcher: IteratorExecutor<T>;
  };

  constructor(fetcher: IteratorExecutor<T>, pipeline?: P) {
    this.config = { pipeline: pipeline as P, fetcher };
  }

  async forEachStride(
    iterator: (items: T[], index: number, pipeline: P, cancel: () => void) => Promise<any> | void
  ): Promise<P> {
    let index = 0;
    const executor = this.config.fetcher.execute();
    let cancelled = false;
    const cancel = () => {
      cancelled = true;
    };

    for await (const stride of executor) {
      await iterator(stride, index, this.config.pipeline, cancel);
      index += 1;

      if (cancelled) {
        break;
      }
    }

    return this.config.pipeline;
  }

  // when a promise is returned, all promises are resolved in the batch before processing the next batch
  async forEach(
    iterator: (item: T, index: number, pipeline: P, cancel: () => void) => Promise<any> | void
  ): Promise<P> {
    let index = 0;
    let iteratorPromises = [];
    const executor = this.config.fetcher.execute();
    let cancelled = false;
    const cancel = () => {
      cancelled = true;
    };

    // eslint-disable-next-line no-labels
    strides: for await (const stride of executor) {
      iteratorPromises = [];
      for (const item of stride) {
        const iteratorResponse = iterator(item, index, this.config.pipeline, cancel);
        index += 1;

        if (cancelled) {
          await Promise.all(iteratorPromises);

          // eslint-disable-next-line no-labels
          break strides;
        } else if (typeof iteratorResponse === "object" && iteratorResponse instanceof Promise) {
          iteratorPromises.push(iteratorResponse);
        }
      }

      await Promise.all(iteratorPromises);
    }

    await Promise.all(iteratorPromises);

    return this.config.pipeline;
  }

  async map<U>(iterator: (item: T, index: number) => U): Promise<U[]> {
    const results: U[] = [];

    const executor = this.config.fetcher.execute();
    let index = 0;

    for await (const stride of executor) {
      for (const item of stride) {
        results.push(iterator(item, index));
        index += 1;
      }
    }

    return results;
  }

  mapLazy<U>(iterator: (item: T, index: number) => U): TableIterator<U, P> {
    const existingFetcher = this.config.fetcher;
    let results: U[] = [];
    let index = 0;

    const fetcher = async function* () {
      const executor = existingFetcher.execute();
      for await (const stride of executor) {
        results = stride.map((item) => {
          const result = iterator(item, index);
          index += 1;
          return result;
        });

        yield results;
      }
    };

    return new TableIterator({ execute: fetcher }, this.config.pipeline);
  }

  all(): Promise<T[]> {
    const result = this.map((i) => i);
    return result;
  }

  async *iterator(): AsyncGenerator<T, void, void> {
    const executor = this.config.fetcher.execute();

    for await (const stride of executor) {
      for (const item of stride) {
        yield item;
      }
    }
  }

  strideIterator(): AsyncGenerator<T[], void, void> {
    return this.config.fetcher.execute();
  }
}
