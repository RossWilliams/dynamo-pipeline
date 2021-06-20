"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TokenBucket = void 0;
class TokenBucket {
    constructor(tableOrIndexName, fillRatePerSecond) {
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
    take(quantity = 1, allowDeficit = false) {
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
    peak() {
        this.refill();
        return this.tokens;
    }
    refill() {
        const now = Date.now();
        const tokensToAdd = Math.floor(((now - this.lastFilled) / 1000) * this.fillRatePerSecond);
        // for now do not consider burst capacity, allow at most 1 second's worth of tokens
        this.tokens = Math.min(this.fillRatePerSecond, this.tokens + tokensToAdd);
        this.lastFilled = now;
    }
}
exports.TokenBucket = TokenBucket;
