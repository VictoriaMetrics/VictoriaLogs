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

// Reject control, format/bidi, separator and invisible code points that can spoof the URL.
const forbiddenURLCharsRegex = /[\p{Cc}\p{Cf}\p{Zs}\p{Zl}\p{Zp}\p{Default_Ignorable_Code_Point}]/u;

export const isAllowedMarkdownLink = (href: string): boolean => {
  if (href === "" || forbiddenURLCharsRegex.test(href)) {
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
