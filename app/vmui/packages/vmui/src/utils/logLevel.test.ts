import { describe, expect, it } from "vitest";
import { LOG_LEVEL_COLORS, LOG_LEVEL_FIELDS, LOG_LEVEL_UNKNOWN } from "../constants/logLevel";
import type { Logs } from "../api/types";
import { getLogLevel, getLogLevelColor } from "./logLevel";

type LogLevel = keyof typeof LOG_LEVEL_COLORS;

const log = (fields: Record<string, unknown>): Logs => fields as Logs;
const logLevels = Object.keys(LOG_LEVEL_COLORS) as LogLevel[];

describe("logLevel utils", () => {
  describe("getLogLevel", () => {
    it("returns unknown for empty log", () => {
      expect(getLogLevel(log({}))).toBe(LOG_LEVEL_UNKNOWN);
    });

    it("returns unknown for unrecognized field and value", () => {
      expect(getLogLevel(log({ foo: "bar" }))).toBe(LOG_LEVEL_UNKNOWN);
    });

    it("returns unknown for numeric value", () => {
      expect(getLogLevel(log({ level: 30 }))).toBe(LOG_LEVEL_UNKNOWN);
    });

    it("returns unknown for null value", () => {
      expect(getLogLevel(log({ level: null }))).toBe(LOG_LEVEL_UNKNOWN);
    });

    it.each(LOG_LEVEL_FIELDS)("detects level from field \"%s\"", (field) => {
      expect(getLogLevel(log({ [field]: "error" }))).toBe("error");
    });

    it("prefers 'level' over 'severity'", () => {
      expect(getLogLevel(log({ level: "debug", severity: "error" }))).toBe("debug");
    });

    it("prefers 'severity' over 'status'", () => {
      expect(getLogLevel(log({ severity: "warn", status: "error" }))).toBe("warn");
    });

    it("skips unknown value and continues checking next fields", () => {
      expect(getLogLevel(log({ level: "custom", severity: "error" }))).toBe("error");
    });

    it("normalizes aliases", () => {
      expect(getLogLevel(log({ level: "verbose" }))).toBe("debug");
      expect(getLogLevel(log({ level: "information" }))).toBe("info");
      expect(getLogLevel(log({ level: "informational" }))).toBe("info");
      expect(getLogLevel(log({ level: "warning" }))).toBe("warn");
      expect(getLogLevel(log({ level: "err" }))).toBe("error");
      expect(getLogLevel(log({ level: "severe" }))).toBe("error");
      expect(getLogLevel(log({ level: "critical" }))).toBe("fatal");
      expect(getLogLevel(log({ level: "crit" }))).toBe("fatal");
      expect(getLogLevel(log({ level: "alert" }))).toBe("fatal");
      expect(getLogLevel(log({ level: "emergency" }))).toBe("fatal");
      expect(getLogLevel(log({ level: "emerg" }))).toBe("fatal");
      expect(getLogLevel(log({ level: "panic" }))).toBe("fatal");
    });

    it("handles case-insensitive values", () => {
      expect(getLogLevel(log({ level: "INFO" }))).toBe("info");
      expect(getLogLevel(log({ level: "Error" }))).toBe("error");
      expect(getLogLevel(log({ level: "Warn" }))).toBe("warn");
    });

    it("trims whitespace from value", () => {
      expect(getLogLevel(log({ level: "  info  " }))).toBe("info");
    });
  });

  describe("getLogLevelColor", () => {
    it("returns neutral light color for unknown level", () => {
      expect(getLogLevelColor(log({}))).toBe(LOG_LEVEL_COLORS[LOG_LEVEL_UNKNOWN]);
    });

    it("returns neutral dark color for unknown level", () => {
      expect(getLogLevelColor(log({}))).toBe(LOG_LEVEL_COLORS[LOG_LEVEL_UNKNOWN]);
    });

    it.each(logLevels.filter((level) => level !== LOG_LEVEL_UNKNOWN))(
      "returns correct light color for level \"%s\"",
      (level) => {
        expect(getLogLevelColor(log({ level }))).toBe(LOG_LEVEL_COLORS[level]);
      },
    );

    it.each(logLevels.filter((level) => level !== LOG_LEVEL_UNKNOWN))(
      "returns correct dark color for level \"%s\"",
      (level) => {
        expect(getLogLevelColor(log({ level }))).toBe(LOG_LEVEL_COLORS[level]);
      },
    );

    it("returns normalized level color", () => {
      expect(getLogLevelColor(log({ level: "warning" }))).toBe(
        LOG_LEVEL_COLORS.warn,
      );
      expect(getLogLevelColor(log({ level: "critical" }))).toBe(
        LOG_LEVEL_COLORS.fatal,
      );
      expect(getLogLevelColor(log({ level: "information" }))).toBe(
        LOG_LEVEL_COLORS.info,
      );
    });
  });
});
