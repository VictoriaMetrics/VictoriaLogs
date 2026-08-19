import { describe, expect, it } from "vitest";
import { getTenantLabel, getTenantSearchString, normalizeTenantId, parseTenantAliases } from "./tenant";

describe("normalizeTenantId", () => {
  it("keeps a canonical accountID:projectID pair", () => {
    expect(normalizeTenantId("0:1")).toBe("0:1");
  });

  it("defaults a missing projectID to 0", () => {
    expect(normalizeTenantId("7")).toBe("7:0");
  });

  it("trims surrounding whitespace and drops leading zeros", () => {
    expect(normalizeTenantId(" 01:002 ")).toBe("1:2");
  });

  it("returns an empty string for malformed ids", () => {
    expect(normalizeTenantId("a:1")).toBe("");
    expect(normalizeTenantId("0:1:2")).toBe("");
    expect(normalizeTenantId("-1:0")).toBe("");
    expect(normalizeTenantId("")).toBe("");
  });
});

describe("parseTenantAliases", () => {
  it("returns an empty map when the config carries no aliases", () => {
    expect(parseTenantAliases(undefined)).toEqual({});
    expect(parseTenantAliases({} as never)).toEqual({});
  });

  it("normalizes tenant ids and trims aliases", () => {
    expect(parseTenantAliases({ tenantAliases: { "0:0": "k8s", "7": " nginx-access " } } as never))
      .toEqual({ "0:0": "k8s", "7:0": "nginx-access" });
  });

  it("drops entries with a malformed tenant id or an empty alias", () => {
    expect(parseTenantAliases({ tenantAliases: { "a:1": "bad", "0:1": "  ", "0:2": "ok" } } as never))
      .toEqual({ "0:2": "ok" });
  });
});

describe("getTenantLabel", () => {
  const aliases = { "0:0": "k8s" };

  it("returns the alias when one is defined", () => {
    expect(getTenantLabel("0:0", aliases)).toBe("k8s");
  });

  it("matches an alias through a non-canonical tenant id", () => {
    expect(getTenantLabel("00:0", aliases)).toBe("k8s");
  });

  it("falls back to the tenant id when there is no alias", () => {
    expect(getTenantLabel("0:1", aliases)).toBe("0:1");
  });
});

describe("getTenantSearchString", () => {
  const aliases = { "0:0": "k8s" };

  it("matches both the alias and the raw tenant id", () => {
    expect(getTenantSearchString("0:0", aliases)).toBe("k8s 0:0");
  });

  it("returns just the tenant id when there is no alias", () => {
    expect(getTenantSearchString("0:1", aliases)).toBe("0:1");
  });
});
