export const escapeHTML = (value: string): string => {
  return value.replace(/[&<>"']/g, (ch) => {
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
};

export const hasInvalidURLChars = (value: string): boolean => {
  for (const ch of value) {
    const code = ch.charCodeAt(0);
    if (code <= 0x20 || code === 0x7f || (code >= 0x80 && code <= 0x9f)) {
      return true;
    }
  }
  return false;
};

export const isAllowedMarkdownLink = (href: string): boolean => {
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

export const isExplicitInlineMarkdownLink = (raw: string): boolean => {
  return raw.startsWith("[") && raw.includes("](") && raw.endsWith(")");
};
