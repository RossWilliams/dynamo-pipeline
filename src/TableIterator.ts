import DynamoDB from "aws-sdk/clients/dynamodb";

interface IteratorExecutor<T> {
  execute(): AsyncGenerator<T[], void, void>;
}
export class TableIterator<P, T = DynamoDB.AttributeMap> {
  config: {
    pipeline: P;
    fetcher: IteratorExecutor<T>;
  };

  constructor(pipeline: P, fetcher: IteratorExecutor<T>) {
    this.config = { pipeline, fetcher };
  }

  // when a promise is returned, all promises are resolved in the batch before processing the next batch
  async forEach(iterator: (item: T, index: number, pipeline: P) => Promise<any> | false | void): Promise<P> {
    let index = 0;
    let iteratorPromises = [];
    const executor = this.config.fetcher.execute();
    // eslint-disable-next-line no-labels
    strides: for await (const stride of executor) {
      await Promise.all(iteratorPromises);
      iteratorPromises = [];
      for (const item of stride) {
        const iteratorResponse = iterator(item, index, this.config.pipeline);
        index += 1;

        // TODO: Improve false return as an early-exit mechanism. not clear to user
        if (iteratorResponse === false) {
          await Promise.all(iteratorPromises);

          // eslint-disable-next-line no-labels
          break strides;
        } else if (typeof iteratorResponse === "object" && iteratorResponse instanceof Promise) {
          iteratorPromises.push(iteratorResponse);
        }
      }
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

  all(): Promise<T[]> {
    const result = this.map((i) => i);
    return result;
  }
}
