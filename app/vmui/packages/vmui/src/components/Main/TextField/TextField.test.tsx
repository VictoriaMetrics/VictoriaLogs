import { render } from "@testing-library/preact";
import { describe, expect, it, vi } from "vitest";
import TextField from "./TextField";

vi.mock("../../../state/common/StateContext", () => ({
  useAppState: () => ({ isDarkTheme: false }),
}));

describe.each(["text", "textarea"] as const)("TextField %s caret updates", type => {
  it("does not reset the caret on rerender without an explicit update", () => {
    const { getByRole, rerender } = render(<TextField
      type={type}
      value="abcdef"
      caretPosition={[2, 2]}
    />);
    const input = getByRole("textbox") as HTMLInputElement | HTMLTextAreaElement;
    input.focus();
    input.setSelectionRange(4, 4);

    rerender(<TextField
      type={type}
      value="abcdef"
      caretPosition={[2, 2]}
    />);

    expect(input.selectionStart).toBe(4);
    expect(input.selectionEnd).toBe(4);
  });

  it("reapplies the same caret position when explicitly requested", () => {
    const { getByRole, rerender } = render(
      <TextField
        type={type}
        value="abcdef"
        caretPosition={[2, 2]}
        caretUpdateId={0}
      />
    );
    const input = getByRole("textbox") as HTMLInputElement | HTMLTextAreaElement;
    input.focus();
    input.setSelectionRange(4, 4);

    rerender(<TextField
      type={type}
      value="abcdef"
      caretPosition={[2, 2]}
      caretUpdateId={1}
    />);

    expect(input.selectionStart).toBe(2);
    expect(input.selectionEnd).toBe(2);
  });

  it("does not move the caret in an unfocused field", () => {
    const { getByRole, rerender } = render(
      <TextField
        type={type}
        value="abcdef"
        caretPosition={[2, 2]}
        caretUpdateId={0}
      />
    );
    const input = getByRole("textbox") as HTMLInputElement | HTMLTextAreaElement;
    input.setSelectionRange(4, 4);

    rerender(<TextField
      type={type}
      value="abcdef"
      caretPosition={[2, 2]}
      caretUpdateId={1}
    />);

    expect(input.selectionStart).toBe(4);
    expect(document.activeElement).not.toBe(input);
  });
});
