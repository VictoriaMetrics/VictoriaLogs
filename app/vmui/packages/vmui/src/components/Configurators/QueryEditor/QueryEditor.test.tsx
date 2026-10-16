import { fireEvent, render, screen } from "@testing-library/preact";
import { useState } from "preact/compat";
import { describe, expect, it, vi } from "vitest";
import QueryEditor, { QueryEditorAutocompleteProps } from "./QueryEditor";

vi.mock("../../../state/common/StateContext", () => ({
  useAppState: () => ({ isDarkTheme: false }),
}));

vi.mock("../../../state/query/QueryStateContext", () => ({
  useQueryState: () => ({ autocomplete: true, autocompleteQuick: false }),
}));

vi.mock("./QueryEditorHotkeysTip", () => ({ default: () => null }));

describe("QueryEditor caret updates", () => {
  it("applies the caret on every completion even when its position is unchanged", () => {
    const completed = "_msg:=\"Product Found\"\n| limit 5";
    const caret = completed.indexOf("\n");
    const Autocomplete = ({ onSelect }: QueryEditorAutocompleteProps) => (
      <button onClick={() => onSelect(completed, caret)}>Complete</button>
    );
    const Editor = () => {
      const [value, setValue] = useState("_msg:=\"Product Found\"");
      return (
        <QueryEditor
          value={value}
          onChange={setValue}
          label="Query"
          autocompleteEl={Autocomplete}
          onEnter={() => {}}
          onArrowUp={() => {}}
          onArrowDown={() => {}}
        />
      );
    };

    render(<Editor/>);
    const input = screen.getByRole("textbox") as HTMLTextAreaElement;
    input.focus();

    for (let attempt = 0; attempt < 2; attempt++) {
      fireEvent.input(input, { target: { value: "_msg:\"Product F\"\n| limit 5" } });
      input.setSelectionRange(15, 15);
      fireEvent.click(screen.getByRole("button", { name: "Complete" }));

      expect(input.value).toBe(completed);
      expect(input.selectionStart).toBe(caret);
      expect(input.selectionEnd).toBe(caret);
    }
  });
});
