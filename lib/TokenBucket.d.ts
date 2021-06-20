export declare class TokenBucket {
    readonly tableOrIndexName: string;
    readonly fillRatePerSecond: number;
    private tokens;
    private lastFilled;
    constructor(tableOrIndexName: string, fillRatePerSecond: number);
    /**
     *
     * @param quantity The number of tokens requested to take
     * @returns [boolean, number] whether there are enough tokens available and the remaining tokens after the take.
     */
    take(quantity?: number, allowDeficit?: boolean): [boolean, number];
    peak(): number;
    private refill;
}
