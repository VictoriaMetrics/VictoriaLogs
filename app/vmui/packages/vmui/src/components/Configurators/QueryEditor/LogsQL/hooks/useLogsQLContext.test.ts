import { act, renderHook } from "@testing-library/preact";
import { describe, expect, it, vi } from "vitest";
import { ContextType } from "../types";
import { generateQuery } from "../helpers/utils";
import { useLogsQLContext } from "./useLogsQLContext";
import { useLogsQLSelect } from "./useLogsQLSelect";

describe.each(["os.type:\"linux\"", "error"])("new filter after %j", prefix => {
  it.each([" ", "  ", "\t", "\n", "\r\n", "\n  ", "\r\n\t", "\n\n\t"])(
    "completes a new field and value after trailing %j without changing the prefix",
    whitespace => {
      const query = prefix + whitespace;
      const onSelect = vi.fn();
      const { result, rerender } = renderHook(props => {
        const context = useLogsQLContext(props);
        const select = useLogsQLSelect({ ...context, onSelect });
        return { ...context, select };
      }, {
        initialProps: { value: query, caretPosition: [query.length, query.length] as [number, number] },
      });

      expect(result.current.contextData).toMatchObject({
        contextType: ContextType.FilterUnknown,
        valueContext: "",
        queryBeforeIncompleteFilter: query,
        position: [query.length, query.length],
      });
      expect(result.current.logicalParts.map(part => `${part.separator}${part.value}`).join("")).toBe(query);

      act(() => result.current.select("service.name", { value: "service.name", type: ContextType.FilterName }));

      const withField = `${query}service.name:`;
      expect(onSelect).toHaveBeenLastCalledWith(withField, withField.length, true);
      rerender({ value: withField, caretPosition: [withField.length, withField.length] });

      expect(result.current.contextData).toMatchObject({
        contextType: ContextType.FilterValue,
        filterName: "service.name",
        valueContext: "",
      });
      expect(generateQuery(result.current.contextData!)).toBe(`${query}service.name:*`);

      act(() => result.current.select("cart", { value: "cart", type: ContextType.FilterValue }));

      const withValue = `${query}service.name:="cart"`;
      expect(onSelect).toHaveBeenLastCalledWith(withValue, withValue.length, false);
    }
  );
});

describe("trailing whitespace inside an existing expression", () => {
  it.each(["os.type:\n", "os.type:in(linux,\n", "(os.type:linux\n", "* | extract "])(
    "does not create a new filter inside %j",
    value => {
      const { result } = renderHook(() => useLogsQLContext({
        value,
        caretPosition: [value.length, value.length],
      }));

      expect(result.current.logicalParts.every(part => part.value !== "")).toBe(true);
    }
  );
});
