import { describe } from "vitest";
import { getWordSelectionAtMouse } from "./utils";

describe("utils", () => {
  describe("getWordSelectionAtMouse", () => {
    it("should return the correct word selection when valid event and position are provided", () => {
      const mockEvent = {
        target: document.createElement("div"),
        clientX: 100,
        clientY: 50,
      } as unknown as MouseEvent;

      const mockTarget = mockEvent.target as HTMLDivElement;
      mockTarget.textContent = "This is a test";
      mockTarget.dataset.id = "5";

      const mockCaretPosition = {
        offsetNode: mockTarget.firstChild,
        offset: 8,
      };

      document.caretPositionFromPoint = vi.fn().mockReturnValue(mockCaretPosition as unknown as CaretPosition);

      const result = getWordSelectionAtMouse(mockEvent);

      expect(result).toEqual({
        start: { elementIndex: 5, positionIndex: 8 },
        end: { elementIndex: 5, positionIndex: 9 },
      });
    });

    it("should return null if the event target is not an HTMLElement", () => {
      const mockEvent = {
        target: document.createTextNode("text"),
        clientX: 100,
        clientY: 50,
      } as unknown as MouseEvent;

      const result = getWordSelectionAtMouse(mockEvent);

      expect(result).toBeNull();
    });

    it("should return null if caret position is not found", () => {
      const mockEvent = {
        target: document.createElement("div"),
        clientX: 100,
        clientY: 50,
      } as unknown as MouseEvent;

      document.caretPositionFromPoint = vi.fn().mockReturnValue(null);

      const result = getWordSelectionAtMouse(mockEvent);

      expect(result).toBeNull();
    });

    it("should extract IP address from string like 'kub.ip: \"10.0.0.3\"'", () => {
      const mockEvent = {
        target: document.createElement("div"),
        clientX: 100,
        clientY: 50,
      } as unknown as MouseEvent;

      const mockTarget = mockEvent.target as HTMLDivElement;
      mockTarget.textContent = "kub.ip: \"10.0.0.3\",";
      mockTarget.dataset.id = "2";

      const mockCaretPosition = {
        offsetNode: mockTarget.firstChild,
        offset: 12,
      };

      document.caretPositionFromPoint = vi.fn().mockReturnValue(mockCaretPosition as unknown as CaretPosition);

      const result = getWordSelectionAtMouse(mockEvent);

      expect(result).toEqual({
        start: { elementIndex: 2, positionIndex: 9 },
        end: { elementIndex: 2, positionIndex: 17 },
      });
    });

    it("should extract word with underscore from string like '\"kubernetes.pod_namespace\": \"vm-operator\"' when clicking on 'n' in namespace", () => {
      const mockEvent = {
        target: document.createElement("div"),
        clientX: 100,
        clientY: 50,
      } as unknown as MouseEvent;

      const mockTarget = mockEvent.target as HTMLDivElement;
      mockTarget.textContent = "\"kubernetes.pod_namespace\": \"vm-operator\",";
      mockTarget.dataset.id = "3";

      const mockCaretPosition = {
        offsetNode: mockTarget.firstChild,
        offset: 16,
      };

      document.caretPositionFromPoint = vi.fn().mockReturnValue(mockCaretPosition as unknown as CaretPosition);

      const result = getWordSelectionAtMouse(mockEvent);

      expect(result).toEqual({
        start: { elementIndex: 3, positionIndex: 12 },
        end: { elementIndex: 3, positionIndex: 25 },
      });
    });
  });
});
