import markedEmoji from "../utils/marked/markedEmoji";
import { marked } from "marked";
import emojis from "./emojis";
import { escapeHTML, isAllowedMarkdownLink, isExplicitInlineMarkdownLink } from "../utils/marked/markedLinks";

// TODO: Dynamically import the emoji map only if the emoji parser is active
marked.use(markedEmoji({ emojis, renderer: (token) => token.emoji }));

marked.use({
  renderer: {
    link({ href, title, tokens, raw }) {
      const text = this.parser.parseInline(tokens);
      if (!isExplicitInlineMarkdownLink(raw) || !isAllowedMarkdownLink(href)) {
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
    // Keep indented/code-like log lines as plain text instead of consuming them as markdown code blocks.
    code() { return undefined; },

    // Keep reference definitions visible in logs instead of treating them as hidden markdown metadata.
    def() { return undefined; },
  }
});
