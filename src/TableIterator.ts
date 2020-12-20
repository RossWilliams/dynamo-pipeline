import { Pipeline } from "./pipeline";
import { BatchFetcher } from "./AbstractFetcher";
import DynamoDB from "aws-sdk/clients/dynamodb";
export class TableIterator<T = DynamoDB.AttributeMap> {
  config: {
    pipeline: Pipeline;
    fetcher: BatchFetcher<T>;
  };

  constructor(pipeline: Pipeline, fetcher: BatchFetcher<T>) {
    this.config = { pipeline, fetcher };
  }

  // when a promise is returned, all promises are resolved in the batch before processing the next batch
  async forEach(iterator: (item: T, pipeline: Pipeline) => Promise<any> | false | void): Promise<Pipeline> {
    let iteratorPromises = [];
    const executor = this.config.fetcher.execute();

    // eslint-disable-next-line no-labels
    strides: for await (const stride of executor) {
      await Promise.all(iteratorPromises);
      iteratorPromises = [];
      for (const item of stride) {
        const iteratorResponse = iterator(item, this.config.pipeline);

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
    const executor = this.config.fetcher.execute();
    const results: U[] = [];
    let index = 0;

    // eslint-disable-next-line no-labels
    for await (const stride of executor) {
      for (const item of stride) {
        results.push(iterator(item, index));
        index += 1;
      }
    }

    return results;
  }

  async all(): Promise<T[]> {
    return this.map((i) => i);
  }
}
