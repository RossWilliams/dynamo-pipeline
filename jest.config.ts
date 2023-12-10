import type { Config } from "jest";

const config: Config = {
  testRegex: "/(test)/.*\\.test\\.[jt]s?$",
  setupFiles: ["./test/jest.setup.ts"],
  moduleFileExtensions: ["mjs", "js", "json", "ts", "node"],
  transform: {
    "^.+\\.(t|j)sx?$": "@swc/jest",
  },
  testPathIgnorePatterns: ["node_modules/", ".buildcache/"],
  verbose: true,
  collectCoverage: true,
  collectCoverageFrom: ["src/**/*.ts", "!src/mocks/*.ts"],
  testTimeout: 90000,
};

export default config;
