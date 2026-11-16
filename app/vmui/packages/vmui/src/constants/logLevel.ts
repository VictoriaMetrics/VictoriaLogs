export const LOG_LEVEL_UNKNOWN = "other";

export const LOG_LEVEL_COLORS = {
  trace: "#20BFC0",
  debug: "#4771E2",
  info: "#62A53B",
  notice: "#62A53B",
  warn: "#F27800",
  error: "#D2323B",
  fatal: "#9C50D3",
  critical: "#9C50D3",
  alert: "#9C50D3",
  emergency: "#9C50D3",
  [LOG_LEVEL_UNKNOWN]: "#A09F9F",
} as const;

export const LOG_LEVEL_ABBREVIATIONS: Partial<Record<keyof typeof LOG_LEVEL_COLORS, string>> = {
  critical: "crit",
  emergency: "emerg",
};

// OTLP severity levels in ascending order of severity. Each level spans four consecutive
// severity numbers: 1-4 trace, 5-8 debug, 9-12 info, 13-16 warn, 17-20 error, 21-24 fatal.
// See https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber
export const OTLP_SEVERITY_LEVELS = ["trace", "debug", "info", "warn", "error", "fatal"] as const;

export const LOG_LEVEL_FIELDS = [
  "level",
  "lvl",
  "log_level",
  "log.level",
  "loglevel",

  "SeverityText",
  "severityText",
  "severity_text",
  "severity",

  "levelname",
  "level_name",

  "@l",
  "@level",

  "logLevel",
  "logLevelName",

  "detected_level",
  "status",

  "syslog.severity.name",
  "log.syslog.severity.name",

  "Level",
  "severityLevel",
  "log_severity",
  "severityValue",
] as const;
