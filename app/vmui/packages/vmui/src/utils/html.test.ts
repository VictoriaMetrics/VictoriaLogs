import { describe, expect, it } from "vitest";
import { markdownToSafeHtml, sanitizeHtml } from "./html";

const onErrorAttribute = "onerror";
const onMouseOverAttribute = "onmouseover";

describe("sanitizeHtml", () => {
  it("should remove scripts and event handlers", () => {
    const result = sanitizeHtml(`
      <script>alert(document.cookie)</script>
      <img src="data:," alt="test" ${onErrorAttribute}="alert(document.cookie)">
      <span ${onMouseOverAttribute}="alert(document.cookie)">text</span>
    `);

    expect(result).not.toContain("<script");
    expect(result).not.toContain("onerror");
    expect(result).not.toContain("onmouseover");
    expect(result).toContain("<img src=\"data:,\" alt=\"test\">");
    expect(result).toContain("<span>text</span>");
  });

  it("should preserve safe relabeling markup", () => {
    const value = "<span title=\"label\">metric_name</span>";

    expect(sanitizeHtml(value)).toBe(value);
  });

  it("should remove inline styles", () => {
    const value = "<span style=\"color:red;position:fixed\" title=\"label\">metric_name</span>";

    expect(sanitizeHtml(value)).toBe("<span title=\"label\">metric_name</span>");
  });

  it("should preserve links opening in a new tab", () => {
    const value = "<a href=\"https://docs.victoriametrics.com/victorialogs/logsql/#block_stats-pipe\" target=\"_blank\" rel=\"noreferrer\">block_stats pipe</a>";

    expect(sanitizeHtml(value)).toBe(value);
  });

  it("should handle an empty string", () => {
    expect(sanitizeHtml("")).toBe("");
  });
});

describe("markdownToSafeHtml", () => {
  it("should render markdown and preserve its safe markup", () => {
    const result = markdownToSafeHtml("# Title\n\n**description**");

    expect(result).toContain("<h1>Title</h1>");
    expect(result).toContain("<strong>description</strong>");
  });

  it("should render raw HTML in markdown as text", () => {
    const result = markdownToSafeHtml(`<img src="data:," alt="test" ${onErrorAttribute}="alert(document.cookie)">`);

    expect(result).toContain("&lt;img src=&quot;data:,&quot; alt=&quot;test&quot; onerror=&quot;alert(document.cookie)&quot;&gt;");
    expect(result).not.toContain("<img");
  });

  it("should render images and non-http links as text", () => {
    const image = markdownToSafeHtml("![alt](https://example.com/image.png)");
    const link = markdownToSafeHtml("[link](mailto:test@example.com)");
    const unsafeLink = markdownToSafeHtml("[link](javascript:alert(1))");

    expect(image).toContain("![alt](https://example.com/image.png)");
    expect(image).not.toContain("<img");
    expect(link).toContain("[link](mailto:test@example.com)");
    expect(link).not.toContain("<a");
    expect(unsafeLink).toContain("[link](javascript:alert(1))");
    expect(unsafeLink).not.toContain("<a");
  });

  it("should preserve explicit http links", () => {
    expect(markdownToSafeHtml("[link](https://example.com)"))
      .toContain("<a href=\"https://example.com\">link</a>");
  });

  it("removes style elements", () => {
    const result = sanitizeHtml(
      "<div>text<style>body { display: none }</style></div>",
    );

    expect(result).not.toContain("<style");
    expect(result).not.toContain("display");
  });
});
