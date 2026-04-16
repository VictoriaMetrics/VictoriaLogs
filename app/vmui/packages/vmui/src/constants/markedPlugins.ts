import markedEmoji from "../utils/marked/markedEmoji";
import { marked } from "marked";
import emojis from "./emojis";

// TODO: Dynamically import the emoji map only if the emoji parser is active
marked.use(markedEmoji({ emojis, renderer: (token) => token.emoji }));

const SAFE_URL_PROTOCOLS = new Set([
  "http:",
  "https:",
  "mailto:",
  "tel:",
]);

const decodeHtmlEntities = (value: string): string => {
  const textarea = document.createElement("textarea");
  textarea.innerHTML = value;
  return textarea.value;
};

const isSafeUrl = (url: string): boolean => {
  const decoded = decodeHtmlEntities(url);
  const normalized = Array.from(decoded)
    .filter((ch) => {
      const code = ch.charCodeAt(0);
      return code > 0x1f && code !== 0x7f && !/\s/.test(ch);
    })
    .join("");
  const schemeMatch = normalized.match(/^([a-zA-Z][a-zA-Z0-9+.-]*:)/);
  return !schemeMatch || SAFE_URL_PROTOCOLS.has(schemeMatch[1].toLowerCase());
};

marked.use({
  walkTokens(token) {
    if (token.type === "html") {
      token.type = "text";
      token.text = token.raw ?? token.text ?? "";
      return;
    }
    if (token.type === "link" || token.type === "image") {
      if (!isSafeUrl(token.href)) {
        token.href = "";
      }
    }
  },
  tokenizer: {
    code() { return undefined; }
  }
});
