import { getPipe, getSlotOptions, pipeSyntax, PipeNode } from "./syntax";

const renderNode = (node: PipeNode): string => {
  if (node.type === "text") {
    return node.value;
  }
  if (node.type === "slot") {
    return `<${node.name}>`;
  }

  return renderNodes(node.value);
};

const renderNodes = (nodes: PipeNode[]): string =>
  nodes.map(renderNode).join("");

const hasSlot = (nodes: PipeNode[]): boolean => {
  return nodes.some(
    (node) =>
      node.type === "slot" || (node.type === "optional" && hasSlot(node.value)),
  );
};

const hasSpecialSlot = (nodes: PipeNode[]): boolean => {
  return nodes.some(
    (node) =>
      (node.type === "slot" && node.special) ||
      (node.type === "optional" && hasSpecialSlot(node.value)),
  );
};

const getTextPrefixes = (nodes: PipeNode[]): string[] => {
  const [node, ...tail] = nodes;
  if (!node) {
    return [];
  }
  if (node.type === "text") {
    return [node.value];
  }
  if (node.type === "optional") {
    return [...getTextPrefixes(node.value), ...getTextPrefixes(tail)];
  }

  return [];
};

const getPrefixMatch = (value: string, prefix: string): string | undefined => {
  for (let size = Math.min(value.length, prefix.length); size >= 1; size--) {
    const tail = value.slice(-size);
    if (prefix.startsWith(tail)) {
      return tail;
    }
  }

  return undefined;
};

const getBestEffortCompletion = (prefix: string, values: string[]): string => {
  if (!values.length) {
    return "";
  }

  let common = values[0];
  for (const value of values.slice(1)) {
    let size = 0;
    const limit = Math.min(common.length, value.length);
    while (size < limit && common[size] === value[size]) {
      size++;
    }
    common = common.slice(0, size);
    if (!common) {
      break;
    }
  }

  if (common.startsWith(prefix)) {
    const completion = common.slice(prefix.length);
    if (completion) {
      return completion;
    }
  }

  const firstMatch = values.find((value) => value.startsWith(prefix));
  return firstMatch ? firstMatch.slice(prefix.length) : "";
};

const getTextPositionsOutsideQuotes = (
  input: string,
  texts: string[],
  start: number,
): number[] => {
  const positions: number[] = [];
  let quote = "";
  let escaped = false;

  for (let i = start; i <= input.length; i++) {
    const char = input[i];
    if (escaped) {
      escaped = false;
    } else if (char === "\\") {
      escaped = !!quote;
    } else if (quote) {
      if (char === quote) {
        quote = "";
      }
    } else if (char === "\"" || char === "'" || char === "`") {
      quote = char;
    } else if (i > start && texts.some(
      (text) => {
        const rest = input.slice(i);
        return input.startsWith(text, i) || (rest && text.startsWith(rest));
      },
    )) {
      positions.push(i);
    }
  }

  return [...new Set(positions)];
};

const matchOptional = (
  optionalNodes: PipeNode[],
  tail: PipeNode[],
  input: string,
  start: number,
  getOptions: (name: string) => ReturnType<typeof getSlotOptions>,
): string | undefined => {
  const optionalHasSlot = hasSlot(optionalNodes);
  const preferSkip =
    optionalNodes[0]?.type !== "slot" &&
    optionalHasSlot &&
    hasSpecialSlot(tail) &&
    !input.slice(start).trim();

  const skip = (): string | undefined =>
    matchNodes(tail, input, start, getOptions);
  const keep = (): string | undefined =>
    matchNodes([...optionalNodes, ...tail], input, start, getOptions);

  if (preferSkip) {
    return skip() ?? keep();
  }

  const keepGhost = keep();
  if (keepGhost !== undefined) {
    return keepGhost;
  }

  const skipGhost = skip();
  if (skipGhost === undefined) {
    return;
  }
  return skipGhost && !optionalHasSlot
    ? renderNodes(optionalNodes) + skipGhost
    : skipGhost;
};

const matchText = (
  nodes: PipeNode[],
  input: string,
  start: number,
  getOptions: (name: string) => ReturnType<typeof getSlotOptions>,
): string | undefined => {
  const [head, ...tail] = nodes;
  if (head.type !== "text") {
    return;
  }

  if (start === input.length) {
    return head.value + renderNodes(tail);
  }

  const remaining = input.slice(start);
  if (remaining.startsWith(head.value)) {
    return matchNodes(tail, input, start + head.value.length, getOptions);
  }
  if (head.value.startsWith(remaining)) {
    return head.value.slice(remaining.length) + renderNodes(tail);
  }
};

const matchSpecialSlot = (
  nodes: PipeNode[],
  input: string,
  start: number,
  getOptions: (name: string) => ReturnType<typeof getSlotOptions>,
): string | undefined => {
  const [head, ...tail] = nodes;
  if (head.type !== "slot" || !head.special) {
    return;
  }

  const options = getOptions(head.name);
  const value = input.slice(start);
  if (!value) {
    return `<${head.name}>` + renderNodes(tail);
  }

  if (!value.includes("(")) {
    const completion = getBestEffortCompletion(
      value,
      options.map((option) => option.value),
    );
    if (completion) {
      return completion + renderNodes(tail);
    }
  }

  for (const option of options) {
    const ghost = matchNodes(
      [...option.nodes, ...tail],
      input,
      start,
      getOptions,
    );
    if (ghost !== undefined) {
      return ghost;
    }
  }
};

const getSlotEnds = (
  input: string,
  start: number,
  tail: PipeNode[],
): number[] => {
  const texts = getTextPrefixes(tail);
  if (texts.length) {
    const positions = getTextPositionsOutsideQuotes(input, texts, start);
    return positions.length || texts.some((text) => input.startsWith(text, start))
      ? positions
      : [input.length];
  }

  const ends: number[] = [];
  for (let end = input.length; end >= start + 1; end--) {
    ends.push(end);
  }
  return ends;
};

const matchSlot = (
  nodes: PipeNode[],
  input: string,
  start: number,
  getOptions: (name: string) => ReturnType<typeof getSlotOptions>,
): string | undefined => {
  const [head, ...tail] = nodes;
  if (head.type !== "slot") {
    return;
  }
  if (head.special) {
    return matchSpecialSlot(nodes, input, start, getOptions);
  }
  if (start === input.length) {
    return `<${head.name}>` + renderNodes(tail);
  }

  for (const end of getSlotEnds(input, start, tail)) {
    const ghost = matchNodes(tail, input, end, getOptions);
    if (ghost !== undefined) {
      return ghost;
    }
  }

  const tailGhost = renderNodes(tail);
  const prefix = getPrefixMatch(input.slice(start), tailGhost);
  if (prefix) {
    const end = input.length - prefix.length;
    if (end > start) {
      return tailGhost.slice(prefix.length);
    }
  }
};

const matchNodes = (
  nodes: PipeNode[],
  input: string,
  start: number,
  getOptions: (name: string) => ReturnType<typeof getSlotOptions>,
): string | undefined => {
  if (!nodes.length) {
    return start === input.length ? "" : undefined;
  }

  const [head, ...tail] = nodes;
  if (head.type === "optional") {
    return matchOptional(head.value, tail, input, start, getOptions);
  }
  if (head.type === "text") {
    return matchText(nodes, input, start, getOptions);
  }

  return matchSlot(nodes, input, start, getOptions);
};

const getPipeName = (value: string): string => {
  const match = value.match(/^[^\s(]+/);
  return match ? match[0] : "";
};

const getPipeLead = (value: string): number => {
  const match = value.match(/^[ \t]*/);
  return match ? match[0].length : 0;
};

const getPipeMatch = (value: string): string | undefined => {
  const input = value.slice(getPipeLead(value));
  const name = getPipeName(input);
  const pipe = getPipe(name);

  return pipe
    ? matchNodes(pipe.value, input, 0, (slotName) => getSlotOptions(pipe, slotName))
    : undefined;
};

const getPipePrefix = (value: string): string => {
  const token = value.trim();
  if (!token || /\s/.test(token)) {
    return "";
  }

  const matches = pipeSyntax.filter((item) => item.name.startsWith(token));
  const pipeNames = matches.map((item) => item.name);
  const pipe = getPipe(token + getBestEffortCompletion(token, pipeNames));
  if (pipe) {
    return renderNodes(pipe.value).slice(token.length);
  }

  return getBestEffortCompletion(
    token,
    matches.map((item) => renderNodes(item.value)),
  );
};

const getPipeGhost = (value: string): string => {
  return getPipeMatch(value) ?? getPipePrefix(value);
};

const getCurrentPipe = (query: string): string => {
  let pipeIndex = -1;
  let quote = "";
  let escaped = false;
  let depth = 0;

  for (let i = 0; i < query.length; i++) {
    const char = query[i];
    if (escaped) {
      escaped = false;
    } else if (quote) {
      escaped = char === "\\";
      if (char === quote) {
        quote = "";
      }
    } else if (char === "\"" || char === "'" || char === "`") {
      quote = char;
    } else if (char === "(" || char === "[" || char === "{") {
      depth++;
    } else if (char === ")" || char === "]" || char === "}") {
      depth = Math.max(0, depth - 1);
    } else if (char === "|" && depth === 0) {
      pipeIndex = i;
    }
  }

  return pipeIndex >= 0 ? query.slice(pipeIndex + 1) : "";
};

export const getLogsQLGhostText = (query: string): string => {
  return getPipeGhost(getCurrentPipe(query));
};
