import { marked } from "marked";
import "./markedPlugins";
import { describe, expect, it } from "vitest";

describe("markedPlugins", () => {
  it("renders explicit http and https links as active links", () => {
    expect(marked("[http](http://example.com)")).toBe("<p><a href=\"http://example.com\">http</a></p>\n");
    expect(marked("[https](https://example.com/path?q=1)")).toBe("<p><a href=\"https://example.com/path?q=1\">https</a></p>\n");
    expect(marked("[title](https://example.com \"a & b\")")).toBe("<p><a href=\"https://example.com\" title=\"a &amp; b\">title</a></p>\n");
  });

  it("renders auto-links and bare URLs as inert text", () => {
    expect(marked("https://example.com/a")).toBe("<p>https://example.com/a</p>\n");
    expect(marked("<https://example.com/a>")).toBe("<p>&lt;https://example.com/a&gt;</p>\n");
    expect(marked("www.example.com")).toBe("<p>www.example.com</p>\n");
  });

  it("renders non-http links as inert text", () => {
    expect(marked("[js](javascript:alert(1))")).toBe("<p>[js](javascript:alert(1))</p>\n");
    expect(marked("[encoded](jav&#x61;script:alert(1))")).toBe("<p>[encoded](jav&amp;#x61;script:alert(1))</p>\n");
    expect(marked("[data](data:text/plain,hello)")).toBe("<p>[data](data:text/plain,hello)</p>\n");
    expect(marked("[missing-slashes](https:example.com)")).toBe("<p>[missing-slashes](https:example.com)</p>\n");
    expect(marked("[mail](mailto:test@example.com)")).toBe("<p>[mail](mailto:test@example.com)</p>\n");
    expect(marked("[tel](tel:123456)")).toBe("<p>[tel](tel:123456)</p>\n");
    expect(marked("[relative](/docs)")).toBe("<p>[relative](/docs)</p>\n");
  });

  it("rejects URLs with whitespace or control characters instead of normalizing them", () => {
    expect(marked("[space](<https://example.com/a b>)")).toBe("<p>[space](&lt;https://example.com/a b&gt;)</p>\n");
    expect(marked("[tab](<https://example.com/a\tb>)")).toBe("<p>[tab](&lt;https://example.com/a\tb&gt;)</p>\n");
    expect(marked("[script](<https://example.com/a b?x=<script>>)")).toBe("<p>[script](&lt;https://example.com/a b?x=&lt;script&gt;>)</p>\n");
    expect(marked("[encoded-space](https://example.com/a%20b)")).toBe("<p><a href=\"https://example.com/a%20b\">encoded-space</a></p>\n");
  });

  it("renders images as inert text without loading them", () => {
    expect(marked("![alt text](https://example.com/a.png)")).toBe("<p>![alt text](https://example.com/a.png)</p>\n");
    expect(marked("![xss](javascript:alert(1))")).toBe("<p>![xss](javascript:alert(1))</p>\n");
  });

  it("renders raw HTML as text", () => {
    expect(marked("<img src=x onerror=alert(1)>")).toBe("&lt;img src=x onerror=alert(1)&gt;");
  });
});
