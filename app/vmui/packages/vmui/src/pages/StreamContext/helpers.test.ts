import { describe, expect, it, vi } from "vitest";
import { Logs } from "../../api/types";
import {
  buildContextQuery,
  mergeContextLogs,
} from "./helpers";

describe("StreamContext helpers", () => {
  describe("buildContextQuery", () => {
    const log = {
      _stream_id: "stream-id",
      _time: "2025-01-01T10:00:00.123Z",
      _msg: "",
      _stream: "",
    } as Logs;

    it("builds a query for older logs", () => {
      expect(buildContextQuery(log, "before", 10)).toBe(
        "_stream_id:stream-id _time:<=2025-01-01T10:00:00.123000000Z | sort by (_time) desc limit 10"
      );
    });

    it("builds a query for newer logs", () => {
      expect(buildContextQuery(log, "after", 10)).toBe(
        "_stream_id:stream-id _time:>=2025-01-01T10:00:00.123000000Z | sort by (_time) asc limit 10"
      );
    });

    it("throws if _stream_id or _time is missing", () => {
      expect(() => buildContextQuery({ ...log, _stream_id: "" }, "after", 10))
        .toThrow("Log must contain _stream_id and _time fields.");

      expect(() => buildContextQuery({ ...log, _time: "" }, "after", 10))
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

    it("removes only the first anchor log when multiple logs share the same timestamp", () => {
      const setter = vi.fn();
      const sameTimestampLog = {
        ...target,
        _msg: "same timestamp",
      } as Logs;

      mergeContextLogs("after", setter)([target, sameTimestampLog, newerLog], target);

      const updater = setter.mock.calls[0][0];
      const result = updater([]);

      expect(result).toEqual([sameTimestampLog, newerLog]);
    });

    it("prepends before logs and keeps additional logs with the same timestamp", () => {
      const setter = vi.fn();
      const sameTimestampLog = {
        ...target,
        _msg: "same timestamp",
      } as Logs;

      mergeContextLogs("before", setter)([olderLog, target, sameTimestampLog], target);

      const updater = setter.mock.calls[0][0];
      const result = updater([]);

      expect(result).toEqual([olderLog, sameTimestampLog]);
    });
  });
});
