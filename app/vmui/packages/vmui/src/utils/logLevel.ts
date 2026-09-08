import { LOG_LEVEL_COLORS, LOG_LEVEL_FIELDS, LOG_LEVEL_UNKNOWN } from "../constants/logLevel";
import type { Logs } from "../api/types";

type LogLevel = keyof typeof LOG_LEVEL_COLORS;
type LogLevelColor = (typeof LOG_LEVEL_COLORS)[LogLevel];

const LEVEL_NORMALIZE: Record<string, LogLevel> = {
  trace: "trace",

  debug: "debug",
  verbose: "debug",

  info: "info",
  information: "info",
  informational: "info",
  notice: "info",

  warn: "warn",
  warning: "warn",

  error: "error",
  err: "error",
  severe: "error",

  fatal: "fatal",
  critical: "fatal",
  crit: "fatal",
  alert: "fatal",
  emergency: "fatal",
  emerg: "fatal",
  panic: "fatal",
};

const normalizeLogLevel = (value: unknown): LogLevel | null => {
  if (typeof value !== "string") {
    return null;
  }

  return LEVEL_NORMALIZE[value.trim().toLowerCase()] ?? null;
};

export const getLogLevel = (log: Logs): LogLevel => {
  for (const field of LOG_LEVEL_FIELDS) {
    const level = normalizeLogLevel(log[field]);

    if (level) {
      return level;
    }
  }

  return LOG_LEVEL_UNKNOWN;
};

export const getLogLevelColor = (log: Logs): LogLevelColor => {
  return LOG_LEVEL_COLORS[getLogLevel(log)];
};
