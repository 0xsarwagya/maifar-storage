import pino from "pino";

const level = process.env.LOG_LEVEL?.trim().toLowerCase() || "info";
const usePretty = process.env.LOG_PRETTY !== "false";

const transport =
  usePretty && process.stdout.isTTY
    ? pino.transport({
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "SYS:standard",
          ignore: "pid,hostname",
        },
      })
    : undefined;

export const logger = pino(
  {
    level,
    base: undefined,
  },
  transport,
);

export type Logger = typeof logger;
