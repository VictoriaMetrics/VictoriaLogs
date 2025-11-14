/**
 * Extracts the "## Pipes" section from docs/victorialogs/logsql.md
 */

import { access, constants, mkdir, readFile, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { marked } from "marked";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PKG_ROOT = path.resolve(__dirname, "..");
const DOCS_REL = path.join("docs", "victorialogs", "logsql.md");
const OUTPUT_DIR_REL = path.join("src", "generated");
const OUTPUT_FILE = "logsql.pipes.ts";

/** @see {@link baseOutput} - keep in sync if fields change */
type PipeEntry = {
  id: string; // slug from title (must end with pipe)
  title: string;  // H3 text without '#'
  description: string; // HTML from the Markdown body
  value: string; // function name to insert after "|"
};

const baseOutput = `/**
 * Do not edit by hand.
 * Auto-generated from docs/victorialogs/logsql.md (## Pipes)
 * Committed on purpose for build stability (no-gen environments). Regenerate via \`npm run gen:logsql-pipes\`.
 * Source: scripts/extract-pipes.ts
 */

export type PipeEntry = {
  id: string; // slug from title (must end with pipe)
  title: string;  // H3 text without '#'
  description: string; // HTML from the Markdown body
  value: string; // function name to insert after "|"
};`;

function slugify(s: string): string {
  return s
    .trim()
    .toLowerCase()
    .replace(/#/g, "")                       // drop '#'
    .replace(/[`~!?()[\]{}'".,:*+]/g, "")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/**
 * Walks up from startDir until it finds a directory containing docs/victorialogs/logsql.md.
 */
async function findRepoRootWithDocs(startDir: string): Promise<string> {
  let dir = startDir;

  while (true) {
    const candidate = path.join(dir, DOCS_REL);
    try {
      await access(candidate, constants.F_OK);
      return dir;
    } catch {
      const parent = path.dirname(dir);
      if (parent === dir) throw new Error(`Could not locate ${DOCS_REL} by walking up from ${startDir}`);
      dir = parent;
    }
  }
}

/** Tries to infer a function name from the title by reading the first inline code token `func`. */
function inferValueFromTitle(title: string): string | null {
  const m = title.match(/`([^`]+)`/);
  if (m) {
    const candidate = m[1].trim();
    if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(candidate)) return candidate;
  }
  return null;
}

/** Fallback function name derived from id: strip "-pipe" and replace '-' with '_' */
function fallbackValueFromId(id: string): string {
  const base = id.replace(/-pipe$/, "");
  return base.replace(/-+/g, "_");
}

async function extractPipes() {
  // package root = one level up from /scripts
  const pkgRoot = path.resolve(__dirname, "..");
  const repoRoot = await findRepoRootWithDocs(pkgRoot);

  const srcPath = path.join(repoRoot, DOCS_REL);
  const outDir = path.join(PKG_ROOT, OUTPUT_DIR_REL);
  const outTS  = path.join(outDir, OUTPUT_FILE);

  await mkdir(outDir, { recursive: true });
  const content = await readFile(srcPath, "utf8");

  // extract "## Pipes" section up to the next H2
  const pipesHeader = /^##\s*Pipes\b.*$/m;
  const altHeader = /^##Pipes\b.*$/m;
  const startMatch = content.match(pipesHeader) || content.match(altHeader);
  if (!startMatch) throw new Error("Section \"## Pipes\" not found in source file.");

  const startIndex = content.indexOf(startMatch[0]);
  const nextH2 = /^##\s+.*$/gm;
  nextH2.lastIndex = startIndex + startMatch[0].length;
  const next = nextH2.exec(content);
  const endIndex = next ? next.index : content.length;

  const pipesSection = content.slice(startIndex, endIndex).trimEnd() + "\n";

  // strip the leading H2 line itself
  const sectionBody = pipesSection.replace(/^##[^\n]*\n+/, "");

  // find H3 positions
  const h3Regex = /^###\s*(.+?)\s*$/gm;
  const h3Positions: Array<{ title: string; index: number }> = [];
  let m: RegExpExecArray | null;
  while ((m = h3Regex.exec(sectionBody)) !== null) {
    h3Positions.push({ title: m[1], index: m.index });
  }

  const entries: PipeEntry[] = [];
  for (let i = 0; i < h3Positions.length; i++) {
    const cur = h3Positions[i];
    const nextIdx = (i + 1 < h3Positions.length) ? h3Positions[i + 1].index : sectionBody.length;

    // current block range
    const block = sectionBody.slice(cur.index, nextIdx);

    // body without leading H3 line
    const body = block.replace(/^###[^\n]*\n?/, "");

    // title cleanup and slug
    const cleanTitle = cur.title.replace(/#/g, "").trim();
    const id = slugify(cleanTitle);

    // filter: slug must end with "-pipe" and must not contain "conditional"
    if (!id.endsWith("-pipe")) continue;
    if (id.includes("conditional")) continue;

    // html description
    const description = marked(body.trimEnd()) as string;

    // pipe function name
    const value = inferValueFromTitle(cleanTitle) ?? fallbackValueFromId(id);

    entries.push({
      id,
      value,
      title: cleanTitle,
      description: `<div class='vm-markdown'>${description}</div>`,
    });
  }

  entries.sort((a, b) => a.id.localeCompare(b.id));

  const outputSource = `${baseOutput}
export const pipes: PipeEntry[] = ${JSON.stringify(entries, null, 2)} as const;
`;

  await writeFile(outTS, outputSource, "utf8");
  console.info(`Saved ${entries.length} pipe entries to ${path.relative(PKG_ROOT, outTS)} (source: ${path.relative(repoRoot, srcPath)})`);
}

extractPipes().catch((err) => {
  const RED = "\x1b[31m", RESET = "\x1b[0m";
  const msg = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
  console.error(`\n${RED}✖ scripts/extractPipes.ts: ${msg}${RESET}\n`);
  process.exit(1);
});
