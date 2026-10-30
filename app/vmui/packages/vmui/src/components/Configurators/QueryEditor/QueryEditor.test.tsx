import { fireEvent, render, screen } from "@testing-library/preact";
import { ReactNode, useState } from "preact/compat";
import { describe, expect, it, vi } from "vitest";
import QueryEditor, { QueryEditorAutocompleteProps } from "./QueryEditor";
import Autocomplete from "../../Main/Autocomplete/Autocomplete";
import { ContextType } from "./LogsQL/types";
import { useLogsQLContext } from "./LogsQL/hooks/useLogsQLContext";
import { useLogsQLSelect } from "./LogsQL/hooks/useLogsQLSelect";

vi.mock("../../../state/common/StateContext", () => ({
  useAppState: () => ({ isDarkTheme: false }),
}));

vi.mock("../../../state/query/QueryStateContext", () => ({
  useQueryState: () => ({ autocomplete: true, autocompleteQuick: false }),
}));

vi.mock("./QueryEditorHotkeysTip", () => ({ default: () => null }));

vi.mock("../../Main/Popper/Popper", () => ({
  default: ({ open, children }: { open: boolean; children: ReactNode }) => open ? <div>{children}</div> : null,
}));

describe("QueryEditor caret updates", () => {
  it.each(["mouse", "keyboard"])("shows values after selecting a field on a blank last line using %s", selection => {
    const prefix = "os.type:\"linux\"\n";
    const TestAutocomplete = ({ value, caretPosition, anchorEl, onSelect }: QueryEditorAutocompleteProps) => {
      const context = useLogsQLContext({ value, caretPosition });
      const select = useLogsQLSelect({ ...context, onSelect });
      const options = context.contextData?.contextType === ContextType.FilterValue
        ? [{ value: "cart", type: ContextType.FilterValue }]
        : [{ value: "service.name", type: ContextType.FilterName }];

      return (
        <Autocomplete
          value={context.contextData?.valueContext ?? ""}
          options={options}
          anchor={anchorEl}
          minLength={0}
          onSelect={select}
          disabledFullScreen
        />
      );
    };
    const Editor = () => {
      const [value, setValue] = useState(prefix);
      return (
        <QueryEditor
          value={value}
          onChange={setValue}
          label="Query"
          autocompleteEl={TestAutocomplete}
          onEnter={() => {}}
          onArrowUp={() => {}}
          onArrowDown={() => {}}
        />
      );
    };

    render(<Editor/>);
    const input = screen.getByRole("textbox") as HTMLTextAreaElement;
    input.focus();
    input.setSelectionRange(prefix.length, prefix.length);
    fireEvent.keyUp(input, { key: "ArrowRight" });
    if (selection === "keyboard") {
      fireEvent.keyDown(input, { key: "ArrowDown" });
      fireEvent.keyDown(input, { key: "Enter" });
    } else {
      fireEvent.click(screen.getByText("service.name", { exact: true }));
    }

    expect(input.value).toBe(`${prefix}service.name:`);
    expect(screen.getByText("cart", { exact: true })).toBeInTheDocument();
    if (selection === "keyboard") {
      fireEvent.keyDown(input, { key: "ArrowDown" });
      fireEvent.keyDown(input, { key: "Enter" });
    } else {
      fireEvent.click(screen.getByText("cart", { exact: true }));
    }

    expect(input.value).toBe(`${prefix}service.name:="cart"`);
    expect(input.selectionStart).toBe(input.value.length);
    expect(input.selectionEnd).toBe(input.value.length);
  });

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
