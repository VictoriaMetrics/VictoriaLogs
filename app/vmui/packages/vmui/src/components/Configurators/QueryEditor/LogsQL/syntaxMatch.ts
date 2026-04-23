import { getPipe, getSlotOptions, pipeSyntax, PipeNode } from "./syntax";
import { splitLogicalParts } from "./parser";
import { LogicalPartType } from "./types";

interface SlotInput {
  name: string;
  special: boolean;
  start: number;
  end: number;
  value: string;
}

interface PipeMatch {
  ghost: string;
  slot?: SlotInput;
}

const renderNode = (node: PipeNode): string => {
  if (node.type === "text") {
    return node.value;
  }
  if (node.type === "slot") {
    return `<${node.name}>`;
  }

  return node.value.map(renderNode).join("");
};

const renderNodes = (value: PipeNode[]): string =>
  value.map(renderNode).join("");

const hasSpecialSlot = (nodes: PipeNode[]): boolean => {
  return nodes.some(
    (node) =>
      (node.type === "slot" && node.special) ||
      (node.type === "optional" && hasSpecialSlot(node.value)),
  );
};

const renderGhostNodes = (nodes: PipeNode[]): string => {
  let result = "";
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    if (node.type === "slot" && node.special) {
      result += `<${node.name}>`;
      continue;
    }
    if (node.type === "optional") {
      if (hasSlot(node.value) && hasSpecialSlot(nodes.slice(i + 1))) {
        continue;
      }
      result += renderGhostNodes(node.value);
      continue;
    }

    result += renderNode(node);
  }

  return result;
};

const hasSlot = (nodes: PipeNode[]): boolean => {
  return nodes.some(
    (node) =>
      node.type === "slot" || (node.type === "optional" && hasSlot(node.value)),
  );
};

const getRequiredTextPrefix = (nodes: PipeNode[]): string => {
  return nodes[0]?.type === "text" ? nodes[0].value : "";
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

const matchNodes = (
  nodes: PipeNode[],
  input: string,
  start: number,
  getOptions: (name: string) => ReturnType<typeof getSlotOptions>,
): PipeMatch | undefined => {
  if (!nodes.length) {
    return start === input.length ? { ghost: "" } : undefined;
  }

  const [head, ...tail] = nodes;

  if (head.type === "optional") {
    const skip = matchNodes(tail, input, start, getOptions);
    const keep = matchNodes([...head.value, ...tail], input, start, getOptions);
    if (
      start === input.length &&
      head.value[0]?.type === "text" &&
      keep?.ghost &&
      !skip?.ghost
    ) {
      return keep;
    }
    if (start === input.length && head.value[0]?.type === "slot" && skip?.ghost) {
      return skip;
    }

    if (keep?.slot) {
      return keep;
    }
    if (skip) {
      return skip.ghost && !hasSlot(head.value)
        ? {
          ...skip,
          ghost: renderNodes(head.value) + skip.ghost,
        }
        : skip;
    }

    return keep;
  }

  if (head.type === "text") {
    if (start === input.length) {
      return {
        ghost: head.value + renderGhostNodes(tail),
      };
    }

    const remain = input.slice(start);
    if (remain.startsWith(head.value)) {
      return matchNodes(tail, input, start + head.value.length, getOptions);
    }

    if (head.value.startsWith(remain)) {
      return {
        ghost: head.value.slice(remain.length) + renderGhostNodes(tail),
      };
    }

    return undefined;
  }

  if (head.special) {
    const options = getOptions(head.name);
    const value = input.slice(start);

    if (!value) {
      return {
        ghost: `<${head.name}>` + renderGhostNodes(tail),
        slot: {
          name: head.name,
          special: head.special,
          start,
          end: start,
          value: "",
        },
      };
    }

    if (!value.includes("(")) {
      const optionValues = options.map((option) => option.value);
      const completion = getBestEffortCompletion(value, optionValues);
      if (completion) {
        return {
          ghost: completion + renderGhostNodes(tail),
          slot: {
            name: head.name,
            special: head.special,
            start,
            end: input.length,
            value,
          },
        };
      }
    }

    for (const option of options) {
      const match = matchNodes([...option.nodes, ...tail], input, start, getOptions);
      if (match) {
        return match;
      }
    }

    return undefined;
  }

  if (start === input.length) {
    return {
      ghost: `<${head.name}>` + renderGhostNodes(tail),
      slot: {
        name: head.name,
        special: head.special,
        start,
        end: start,
        value: "",
      },
    };
  }

  const requiredTextPrefix = getRequiredTextPrefix(tail);
  const requiredTextStart = requiredTextPrefix
    ? input.indexOf(requiredTextPrefix[0], start)
    : -1;
  const maxEnd = requiredTextStart >= 0 ? requiredTextStart : input.length;

  for (let end = maxEnd; end >= start + 1; end--) {
    const value = input.slice(start, end);
    const next = matchNodes(tail, input, end, getOptions);
    if (!next) {
      continue;
    }

    const match: PipeMatch = {
      ghost: next.ghost,
      slot: next.slot,
    };

    if (end === input.length && !next.slot) {
      match.slot = {
        name: head.name,
        special: head.special,
        start,
        end,
        value,
      };
    }

    return match;
  }

  const ghostTail = renderGhostNodes(tail);
  const prefix = getPrefixMatch(input.slice(start), ghostTail);
  if (prefix) {
    const end = input.length - prefix.length;
    if (end > start) {
      return {
        ghost: ghostTail.slice(prefix.length),
        slot: {
          name: head.name,
          special: head.special,
          start,
          end,
          value: input.slice(start, end),
        },
      };
    }
  }

  return undefined;
};

const getPipeName = (value: string): string => {
  const match = value.match(/^[^\s(]+/);
  return match ? match[0] : "";
};

const getPipeLead = (value: string): number => {
  const match = value.match(/^[ \t]*/);
  return match ? match[0].length : 0;
};

const getPipeMatch = (value: string): PipeMatch | undefined => {
  const lead = getPipeLead(value);
  const input = value.slice(lead);
  const name = getPipeName(input);
  const pipe = getPipe(name);

  if (!pipe) {
    return;
  }

  const match = matchNodes(
    pipe.value,
    input,
    0,
    (slotName) => getSlotOptions(pipe, slotName),
  );
  if (!match?.slot) {
    return match;
  }

  return {
    ...match,
    slot: {
      ...match.slot,
      start: match.slot.start + lead,
      end: match.slot.end + lead,
    },
  };
};

const getPipePrefix = (value: string): string => {
  const token = value.trim();
  if (!token || /\s/.test(token)) {
    return "";
  }

  const matches = pipeSyntax.filter((item) => item.name.startsWith(token));
  if (!matches.length) {
    return "";
  }

  return getBestEffortCompletion(
    token,
    matches.map((item) => renderNodes(item.value)),
  );
};

export const getPipeGhost = (value: string): string => {
  const match = getPipeMatch(value);
  if (match) {
    return match.ghost;
  }

  return getPipePrefix(value);
};

export const getLogsQLGhostText = (query: string): string => {
  const part = splitLogicalParts(query)
    .reverse()
    .find(
      (item) =>
        item.type === LogicalPartType.Pipe ||
        item.type === LogicalPartType.FilterOrPipe,
    );

  if (!part) {
    return "";
  }

  return getPipeGhost(part.value);
};
