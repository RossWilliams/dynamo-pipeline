interface TokenBucketConfig {
  action: "read" | "write";
  fillRatePerSecond: number;
}

export class TokenBucket {
  public readonly tableOrIndexName: string;
  public readonly fillRatePerSecond: number; // total number of tokens available
  private tokens: number; // available tokens
  private lastFilled: number; // milli seconds since epoc

  constructor(tableOrIndexName: string, fillRatePerSecond: number) {
    this.tableOrIndexName = tableOrIndexName;
    this.lastFilled = Date.now();
    this.tokens = fillRatePerSecond;
    this.fillRatePerSecond = fillRatePerSecond;
  }

  /**
   *
   * @param quantity The number of tokens requested to take
   * @returns [boolean, number] whether there are enough tokens available and the remaining tokens after the take.
   */
  take(quantity = 1, allowDeficit = false): [boolean, number] {
    // lazy fill bucket
    this.refill();

    if (this.tokens >= quantity) {
      this.tokens -= quantity;
      return [true, this.tokens];
    }
    if (allowDeficit) {
      this.tokens -= quantity;
    }

    return [false, this.tokens];
  }

  peak(): number {
    this.refill();
    return this.tokens;
  }

  private refill() {
    const now = Date.now();

    const tokensToAdd = Math.floor(((now - this.lastFilled) / 1000) * this.fillRatePerSecond);

    // for now do not consider burst capacity, allow at most 1 second's worth of tokens
    this.tokens = Math.min(this.fillRatePerSecond, this.tokens + tokensToAdd);
    this.lastFilled = now;
  }
}
