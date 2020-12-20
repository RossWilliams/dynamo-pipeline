global.console = {
  ...global.console,
  // log: jest.fn(), // console.log are ignored in tests
  error: jest.fn(),
  warn: jest.fn(),
  info: console.info,
  debug: console.debug,
};
