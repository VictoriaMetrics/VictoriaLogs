import { act, renderHook } from "@testing-library/preact";
import { describe, expect, it, vi } from "vitest";
import { getContextData, splitLogicalParts } from "../helpers/parser";
import { ContextType } from "../types";
import { useLogsQLSelect } from "./useLogsQLSelect";

describe("useLogsQLSelect caret position", () => {
  it.each([
    ["os.type:\"lin▏\"\n_time:5m", "linux", ContextType.FilterValue, "os.type:=\"linux\"▏\n_time:5m"],
    ["os.type:lin▏\n_time:5m", "linux", ContextType.FilterValue, "os.type:=\"linux\"▏\n_time:5m"],
    ["os.type:\"lin▏\"\r\n\t_time:5m", "linux", ContextType.FilterValue, "os.type:=\"linux\"▏\r\n\t_time:5m"],
    ["os.type:\"lin▏\"\n\n  _time:5m", "linux", ContextType.FilterValue, "os.type:=\"linux\"▏\n\n  _time:5m"],
    ["os.type:\"lin▏\"\t_time:5m", "linux", ContextType.FilterValue, "os.type:=\"linux\"▏\t_time:5m"],
    ["os.type:=\"linux\"\nservice.name:\"car▏\"", "cart", ContextType.FilterValue, "os.type:=\"linux\"\nservice.name:=\"cart\"▏"],
    ["os.type:=\"linux\"\n\tservice.na▏", "service.name", ContextType.FilterName, "os.type:=\"linux\"\n\tservice.name:▏"],
    ["service.name:cart\nos.type:\"lin▏\"\n_time:5m", "linux", ContextType.FilterValue, "service.name:cart\nos.type:=\"linux\"▏\n_time:5m"],
  ] as const)("preserves adjacent filters when completing %j", (input, insert, type, expected) => {
    const cursor = input.indexOf("▏");
    const logicalParts = splitLogicalParts(input.replace("▏", ""));
    const part = logicalParts.find(part => cursor >= part.position[0] && cursor <= part.position[1])!;
    const contextData = { ...part, ...getContextData(part, cursor - part.position[0]) };
    const onSelect = vi.fn();
    const { result } = renderHook(() => useLogsQLSelect({ contextData, logicalParts, onSelect }));

    act(() => result.current(insert, { value: insert, type }));

    expect(onSelect).toHaveBeenCalledWith(
      expected.replace("▏", ""), expected.indexOf("▏"), type === ContextType.FilterName
    );
  });

  it.each([
    ["error | limi", "error | limit▏"],
    ["error |  limi", "error |  limit▏"],
    ["error |\tlimi", "error |\tlimit▏"],
    ["error |\t\tlimi", "error |\t\tlimit▏"],
    ["error\n| limi", "error\n| limit▏"],
    ["error\n|\tlimi", "error\n|\tlimit▏"],
    ["error |\nlimi", "error |\nlimit▏"],
    ["error |\n  limi", "error |\n  limit▏"],
    ["error |\n\tlimi", "error |\n\tlimit▏"],
    ["error |\t\n  limi", "error |\t\n  limit▏"],
    ["error |\n\n\tlimi", "error |\n\n\tlimit▏"],
    ["error\n|\n\tlimi", "error\n|\n\tlimit▏"],
    ["error\n| sort _time\n| limi", "error\n| sort _time\n| limit▏"],
    ["error | limi | fields _msg", "error | limit▏ | fields _msg"],
    ["error\n| limi\n| fields _msg", "error\n| limit▏\n| fields _msg"],
    ["error\n|\tlimi\n| fields _msg", "error\n|\tlimit▏\n| fields _msg"],
    ["error |\n  limi\n| fields _msg", "error |\n  limit▏\n| fields _msg"],
    ["error |\r\n\tlimi\r\n| fields _msg", "error |\r\n\tlimit▏\r\n| fields _msg"],
    ["  error | limi", "error | limit▏"],
    ["error | limi  ", "error | limit▏"],
    ["error | limi  | fields _msg", "error | limit▏  | fields _msg"],
    ["\"ошибка\" | limi", "\"ошибка\" | limit▏"],
    ["\"🔥\" | limi", "\"🔥\" | limit▏"],
  ])("places the caret after the selected pipe in %j", (query, expected) => {
    const logicalParts = splitLogicalParts(query);
    const part = logicalParts.find(part => part.value.includes("limi"))!;
    const contextData = {
      ...part,
      ...getContextData(part, part.value.indexOf("limi") + 4),
    };
    const onSelect = vi.fn();
    const { result } = renderHook(() => useLogsQLSelect({ contextData, logicalParts, onSelect }));

    act(() => result.current("limit", { value: "limit", type: ContextType.PipeName }));

    expect(onSelect).toHaveBeenCalledWith(expected.replace("▏", ""), expected.indexOf("▏"), false);
  });

  it.each([
    ["os.ty", "os.type", ContextType.FilterName, "os.type:▏", true],
    ["  os.ty", "os.type", ContextType.FilterName, "os.type:▏", true],
    ["os.type:lin _time:5m", "linux", ContextType.FilterValue, "os.type:=\"linux\"▏ _time:5m", false],
  ] as const)("preserves field completion in %j", (query, insert, type, expected, keepOpen) => {
    const logicalParts = splitLogicalParts(query);
    const part = logicalParts[0];
    const contextData = { ...part, ...getContextData(part, part.value.trimEnd().length) };
    const onSelect = vi.fn();
    const { result } = renderHook(() => useLogsQLSelect({ contextData, logicalParts, onSelect }));

    act(() => result.current(insert, { value: insert, type }));

    expect(onSelect).toHaveBeenCalledWith(expected.replace("▏", ""), expected.indexOf("▏"), keepOpen);
  });
});
