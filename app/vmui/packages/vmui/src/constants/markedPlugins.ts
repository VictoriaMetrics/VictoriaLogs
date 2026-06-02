import markedEmoji from "../utils/marked/markedEmoji";
import { marked } from "marked";
import emojis from "./emojis";

// TODO: Dynamically import the emoji map only if the emoji parser is active
marked.use(markedEmoji({ emojis, renderer: (token) => token.emoji }));

const escapeHTML = (value: string): string => value.replace(/[&<>"']/g, (ch) => {
  switch (ch) {
    case "&":
      return "&amp;";
    case "<":
      return "&lt;";
    case ">":
      return "&gt;";
    case "\"":
      return "&quot;";
    case "'":
      return "&#39;";
    default:
      return ch;
  }
});

const hasInvalidURLChars = (value: string): boolean => {
  for (const ch of value) {
    const code = ch.charCodeAt(0);
    if (code <= 0x20 || code === 0x7f) {
      return true;
    }
  }
  return false;
};

const isAllowedMarkdownLink = (href: string): boolean => {
  if (href === "" || hasInvalidURLChars(href)) {
    return false;
  }
  const lowerHref = href.toLowerCase();
  if (!lowerHref.startsWith("http://") && !lowerHref.startsWith("https://")) {
    return false;
  }

  try {
    const url = new URL(href);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
};

marked.use({
  renderer: {
    link({ href, title, tokens, raw }) {
      const text = this.parser.parseInline(tokens);
      if (!raw.startsWith("[") || !isAllowedMarkdownLink(href)) {
        return escapeHTML(raw);
      }

      const titleAttr = title ? ` title="${escapeHTML(title)}"` : "";
      return `<a href="${escapeHTML(href)}"${titleAttr}>${text}</a>`;
    },
    image({ raw }) {
      return escapeHTML(raw);
    },
  },
  walkTokens(token) {
    if (token.type === "html") {
      token.type = "text";
      token.text = token.raw ?? token.text ?? "";
    }
  },
  tokenizer: {
    code() { return undefined; }
  }
});
