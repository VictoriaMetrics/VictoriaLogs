import { describe, expect, it } from "vitest";
import { getLogsQLGhostText } from "./syntaxMatch";

describe("hint", () => {
  describe("pipe name hints", () => {
    it("uses the syntax matcher once the pipe name is complete", () => {
      expect(getLogsQLGhostText("* | st")).toBe("ats by (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stat")).toBe("s by (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats")).toBe(" <func> if (<filter>) as <name>");
    });
  });

  describe("stats-like function hints", () => {
    it("shows generic syntax ghost text for count pipe", () => {
      expect(getLogsQLGhostText("* | count")).toBe("(<fields>) as <name>");
      expect(getLogsQLGhostText("* | count(")).toBe(") as <name>");
      expect(getLogsQLGhostText("* | count()")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | count(*)")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | count(host)")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | count() as")).toBe(" <name>");
      expect(getLogsQLGhostText("* | count() as ")).toBe("<name>");
    });

    it("shows function ghost text for stats", () => {
      expect(getLogsQLGhostText("* | stats c")).toBe("ount([<fields>]) if (<filter>) as <name>");
    });

    it("does not duplicate the separator after stats space", () => {
      expect(getLogsQLGhostText("* | stats ")).toBe("<func> if (<filter>) as <name>");
    });

    it("skips optional parent clauses when matching a stats function", () => {
      expect(getLogsQLGhostText("* | stats co")).toBe("unt([<fields>]) if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats count")).toBe("([<fields>]) if (<filter>) as <name>");
    });

    it("prefers parent by clause before stats function", () => {
      expect(getLogsQLGhostText("* | stats b")).toBe("y (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats by")).toBe(" (<fields>) <func> if (<filter>) as <name>");
      expect(getLogsQLGhostText("* | stats by (")).toBe("<fields>) <func> if (<filter>) as <name>");
    });

    it("shows function ghost text for stats after by clause", () => {
      expect(getLogsQLGhostText("* | stats by (host) c")).toBe("ount([<fields>]) if (<filter>) as <name>");
    });

    it("does not duplicate the separator after stats by clause", () => {
      expect(getLogsQLGhostText("* | stats by (host) ")).toBe("<func> if (<filter>) as <name>");
    });

    it("shows best-effort ghost when count_ matches multiple stats functions", () => {
      expect(getLogsQLGhostText("* | stats count_")).toBe("empty");
    });

    it("shows running_stats function ghost text", () => {
      expect(getLogsQLGhostText("* | running_stats s")).toBe("um(<fields>) as <name>");
    });

    it("shows total_stats function ghost text", () => {
      expect(getLogsQLGhostText("* | total_stats s")).toBe("um(<fields>) as <name>");
    });

    it("returns to the parent stats syntax after a complete stats function", () => {
      expect(getLogsQLGhostText("* | stats count()")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | stats count(*)")).toBe(" as <name>");
      expect(getLogsQLGhostText("* | stats count() ")).toBe("as <name>");
      expect(getLogsQLGhostText("* | stats count() as")).toBe(" <name>");
      expect(getLogsQLGhostText("* | stats by (host) count()")).toBe(" as <name>");
    });
  });
});
