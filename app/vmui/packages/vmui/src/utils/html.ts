import DOMPurify, { type Config } from "dompurify";
import { vmuiMarked } from "../constants/markedPlugins";

const HTML_SANITIZE_CONFIG: Config = {
  USE_PROFILES: { html: true },
  FORBID_TAGS: ["style"],
  FORBID_ATTR: ["style"],
  ADD_ATTR: ["target"],
};

export const sanitizeHtml = (value: string): string => {
  return DOMPurify.sanitize(value, HTML_SANITIZE_CONFIG);
};

export const markdownToSafeHtml = (value: string): string => {
  return sanitizeHtml(vmuiMarked.parse(value) as string);
};
