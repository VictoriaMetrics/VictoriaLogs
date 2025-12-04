import { isMacOs } from "./detect-device";

export const ctrlKeyLabel = isMacOs() ? "⌘" : "Ctrl";

export const altKeyLabel = isMacOs() ? "⌥" : "Alt";
