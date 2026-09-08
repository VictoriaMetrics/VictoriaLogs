export const LOG_LEVEL_UNKNOWN = "other";

export const LOG_LEVEL_COLORS = {
  trace: "#20BFC0",
  debug: "#4771E2",
  info: "#62A53B",
  warn: "#F27800",
  error: "#D2323B",
  fatal: "#9C50D3",
  [LOG_LEVEL_UNKNOWN]: "#A09F9F",
} as const;

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
