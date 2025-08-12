import { describe, expect } from "vitest";
import { convertBytes } from "./bytes";

describe("convertBytes", () => {
  const fileSizeInBytes = 2048;

  describe("default conversion", () => {
    it("should convert to KB", () => {
      const resultKB = convertBytes(fileSizeInBytes);
      expect(resultKB).toBe("2.05 KB");
    });

    it("should convert to MB", () => {
      const resultMB = convertBytes(fileSizeInBytes * 1000);
      expect(resultMB).toBe("2.05 MB");
    });

    it("should throw an error for invalid decimals", () => {
      expect(() => convertBytes(fileSizeInBytes, { decimals: -1 })).toThrowError("Invalid decimals -1");
    });
  });

  describe("conversion to binary units", () => {
    it("should convert to KiB", () => {
      const resultKiB = convertBytes(fileSizeInBytes, { useBinaryUnits: true });
      expect(resultKiB).toBe("2.00 KiB");
    });

    it("should convert to MiB", () => {
      const resultMiB = convertBytes(fileSizeInBytes * 1024, { useBinaryUnits: true });
      expect(resultMiB).toBe("2.00 MiB");
    });
  });

  it("should handle very large bytes (Number.MAX_SAFE_INTEGER)", () => {
    const result = convertBytes(Number.MAX_SAFE_INTEGER);
    expect(result).toBe("9.01 PB");
  });
});
