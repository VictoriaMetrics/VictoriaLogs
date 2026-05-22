import { describe, expect, it, vi } from "vitest";
import { Logs } from "../../api/types";
import { getSecondsFromDuration } from "../../utils/time";
import {
  appendUniqueContextLogs,
  buildContextQuery,
  getNextContextTarget,
  getNextTimeWindow,
  isMaxTimeWindow,
  mergeContextLogs,
  STREAM_CONTEXT_TIME_WINDOW_INITIAL,
  STREAM_CONTEXT_TIME_WINDOW_MAX,
} from "./helpers";

describe("StreamContext helpers", () => {
  describe("isMaxTimeWindow", () => {
    it("checks max time window by seconds", () => {
      expect(isMaxTimeWindow(STREAM_CONTEXT_TIME_WINDOW_INITIAL)).toBe(false);
      expect(isMaxTimeWindow(STREAM_CONTEXT_TIME_WINDOW_MAX)).toBe(true);

      const maxSeconds = getSecondsFromDuration(STREAM_CONTEXT_TIME_WINDOW_MAX);
      expect(isMaxTimeWindow(`${maxSeconds + 1}s`)).toBe(true);
    });
  });

  describe("getNextTimeWindow", () => {
    it("returns a larger time window", () => {
      const nextTimeWindow = getNextTimeWindow(STREAM_CONTEXT_TIME_WINDOW_INITIAL);

      expect(getSecondsFromDuration(nextTimeWindow)).toBeGreaterThan(
        getSecondsFromDuration(STREAM_CONTEXT_TIME_WINDOW_INITIAL)
      );
    });

    it("normalizes time window on unit boundaries", () => {
      expect(getNextTimeWindow("32m")).toBe("1h");
      expect(getNextTimeWindow("16h")).toBe("1d");
    });

    it("does not exceed max time window", () => {
      const nextTimeWindow = getNextTimeWindow(STREAM_CONTEXT_TIME_WINDOW_MAX);

      expect(getSecondsFromDuration(nextTimeWindow)).toBe(
        getSecondsFromDuration(STREAM_CONTEXT_TIME_WINDOW_MAX)
      );
    });
  });

  describe("buildContextQuery", () => {
    const log = {
      _stream_id: "stream-id",
      _time: "2025-01-01T10:00:00.123Z",
      _msg: "",
      _stream: "",
    } as Logs;

    it("builds a stream_context query with time_window", () => {
      const query = buildContextQuery(log, "before", 10, STREAM_CONTEXT_TIME_WINDOW_INITIAL);

      expect(query).toContain("_stream_id:stream-id");
      expect(query).toContain("_time:2025-01-01T10:00:00.123000000Z");
      expect(query).toContain(`stream_context before 10 time_window ${STREAM_CONTEXT_TIME_WINDOW_INITIAL}`);
    });

    it("throws if _stream_id or _time is missing", () => {
      expect(() => buildContextQuery({ ...log, _stream_id: "" }, "after", 10, STREAM_CONTEXT_TIME_WINDOW_INITIAL))
        .toThrow("Log must contain _stream_id and _time fields.");

      expect(() => buildContextQuery({ ...log, _time: "" }, "after", 10, STREAM_CONTEXT_TIME_WINDOW_INITIAL))
        .toThrow("Log must contain _stream_id and _time fields.");
    });
  });

  describe("mergeContextLogs", () => {
    const target = {
      _stream_id: "stream-id",
      _time: "2025-01-01T10:00:00.123Z",
      _msg: "target",
      _stream: "",
    } as Logs;

    const olderLog = {
      _stream_id: "stream-id",
      _time: "2025-01-01T09:59:00.000Z",
      _msg: "older",
      _stream: "",
    } as Logs;

    const newerLog = {
      _stream_id: "stream-id",
      _time: "2025-01-01T10:01:00.000Z",
      _msg: "newer",
      _stream: "",
    } as Logs;

    it("prepends before logs and removes the target log", () => {
      const setter = vi.fn();
      const prev = [{ ...olderLog, _msg: "existing older" }] as Logs[];

      mergeContextLogs("before", setter)([olderLog, target], target);

      const updater = setter.mock.calls[0][0];
      const result = updater(prev);

      expect(result).toEqual([olderLog, prev[0]]);
      expect(result).not.toContain(target);
    });

    it("appends after logs and removes the target log", () => {
      const setter = vi.fn();
      const prev = [{ ...newerLog, _msg: "existing newer" }] as Logs[];

      mergeContextLogs("after", setter)([target, newerLog], target);

      const updater = setter.mock.calls[0][0];
      const result = updater(prev);

      expect(result).toEqual([prev[0], newerLog]);
      expect(result).not.toContain(target);
    });
  });

  describe("appendUniqueContextLogs", () => {
    it("appends unique logs from expanded time windows", () => {
      const firstLog = {
        _stream_id: "stream-id",
        _time: "2025-01-01T10:00:00.000Z",
        _msg: "first",
        _stream: "",
      } as Logs;

      const secondLog = {
        _stream_id: "stream-id",
        _time: "2025-01-01T10:01:00.000Z",
        _msg: "second",
        _stream: "",
      } as Logs;

      expect(appendUniqueContextLogs([firstLog], [firstLog, secondLog])).toEqual([firstLog, secondLog]);
    });
  });

  describe("getNextContextTarget", () => {
    const oldestLog = {
      _stream_id: "stream-id",
      _time: "2025-01-01T09:59:00.000Z",
      _msg: "oldest",
      _stream: "",
    } as Logs;

    const newestLog = {
      _stream_id: "stream-id",
      _time: "2025-01-01T10:01:00.000Z",
      _msg: "newest",
      _stream: "",
    } as Logs;

    it("uses the newest log as the next target when loading after", () => {
      expect(getNextContextTarget([oldestLog, newestLog], "after")).toBe(newestLog);
    });

    it("uses the oldest log as the next target when loading before", () => {
      expect(getNextContextTarget([oldestLog, newestLog], "before")).toBe(oldestLog);
    });
  });
});
