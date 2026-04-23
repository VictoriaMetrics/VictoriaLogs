export interface PipeOption {
  value: string;
  nodes: PipeNode[];
}

export interface PipeText {
  type: "text";
  value: string;
}

export interface PipeSlot {
  type: "slot";
  name: string;
  special: boolean;
}

export interface PipeOptional {
  type: "optional";
  value: PipeNode[];
}

export type PipeNode = PipeText | PipeSlot | PipeOptional;

export interface PipeItem {
  name: string;
  syntax: string;
  value: PipeNode[];
  [slotName: string]: string | boolean | PipeNode[] | PipeOption[] | undefined;
}

interface PipeRow {
  name: string;
  syntax: string;
  [slotName: string]: string | Omit<PipeOption, "nodes">[] | undefined;
}

const pipeRows: PipeRow[] = [
  { name: "block_stats", syntax: "block_stats" },
  { name: "blocks_count", syntax: "blocks_count" },
  { name: "collapse_nums", syntax: "collapse_nums [at <field>] [prettify]" },
  { name: "count", syntax: "count([<fields>]) [as <name>]" },
  { name: "copy", syntax: "copy <src> [as] <dst>" },
  { name: "decolorize", syntax: "decolorize [<field>]" },
  { name: "delete", syntax: "delete <field>" },
  { name: "drop_empty_fields", syntax: "drop_empty_fields" },
  { name: "extract_regexp", syntax: "extract_regexp <pattern> [from <field>]" },
  { name: "extract", syntax: "extract <pattern> [from <field>]" },
  { name: "facets", syntax: "facets" },
  { name: "field_names", syntax: "field_names [filter <match>] [as <name>]" },
  { name: "field_values", syntax: "field_values <field> [filter <match>] [limit <limit>]" },
  { name: "fields", syntax: "fields <field>" },
  { name: "filter", syntax: "filter <expr>" },
  { name: "first", syntax: "first <limit> [by <field>]" },
  { name: "format", syntax: "format <pattern> [as <name>]" },
  { name: "generate_sequence", syntax: "generate_sequence <count>" },
  { name: "hash", syntax: "hash <field> [as <name>]" },
  { name: "join", syntax: "join (<query>)" },
  { name: "json_array_len", syntax: "json_array_len <field> [as <name>]" },
  { name: "last", syntax: "last <limit> [by <field>]" },
  { name: "len", syntax: "len <field> [as <name>]" },
  { name: "limit", syntax: "limit <value>" },
  { name: "math", syntax: "math <expr>" },
  { name: "offset", syntax: "offset <value>" },
  { name: "pack_json", syntax: "pack_json [fields (<field>)] [as <name>]" },
  { name: "pack_logfmt", syntax: "pack_logfmt [fields (<field>)] [as <name>]" },
  { name: "query_stats", syntax: "query_stats" },
  { name: "rename", syntax: "rename <src> [as] <dst>" },
  { name: "replace_regexp", syntax: "replace_regexp (<pattern>, <replacement>) [at <field>]" },
  { name: "replace", syntax: "replace (<old>, <new>) [at <field>]" },
  {
    name: "running_stats",
    syntax: "running_stats [by (<fields>)] <.func> [as <name>]",
    func: [
      { value: "count([<fields>])" },
      { value: "first(<field>) [offset <offset>]" },
      { value: "last(<field>) [offset <offset>]" },
      { value: "max(<fields>)" },
      { value: "min(<fields>)" },
      { value: "sum(<fields>)" },
    ],
  },
  { name: "sample", syntax: "sample <ratio>" },
  { name: "set_stream_fields", syntax: "set_stream_fields <field>" },
  { name: "sort", syntax: "sort by (<field>)" },
  { name: "split", syntax: "split <separator> [from <src>] [as <dst>]" },
  {
    name: "stats",
    syntax: "stats [by (<fields>)] <.func> [if (<filter>)] [as <name>]",
    func: [
      { value: "any(<field>)" },
      { value: "avg(<field>)" },
      { value: "count([<fields>])" },
      { value: "count_empty(<fields>)" },
      { value: "count_uniq(<fields>) [limit <limit>]" },
      { value: "count_uniq_hash(<fields>) [limit <limit>]" },
      { value: "field_max(<value_field>, <by_field>)" },
      { value: "field_min(<value_field>, <by_field>)" },
      { value: "histogram(<field>)" },
      { value: "json_values([<fields>]) [sort by (<fields>)] [limit <limit>]" },
      { value: "max(<fields>)" },
      { value: "median(<field>)" },
      { value: "min(<fields>)" },
      { value: "quantile(<phi>, <field>)" },
      { value: "rate([<field>])" },
      { value: "rate_sum(<field>)" },
      { value: "row_any([<fields>])" },
      { value: "row_max(<field>[, <fields>])" },
      { value: "row_min(<field>[, <fields>])" },
      { value: "stddev(<field>)" },
      { value: "sum(<fields>)" },
      { value: "sum_len([<fields>])" },
      { value: "uniq_values([<fields>]) [limit <limit>]" },
      { value: "values([<fields>]) [limit <limit>]" },
    ],
  },
  { name: "stream_context", syntax: "stream_context before <lines>" },
  { name: "time_add", syntax: "time_add <offset> [to <field>] [as <name>]" },
  { name: "top", syntax: "top <limit> [by <field>]" },
  {
    name: "total_stats",
    syntax: "total_stats [by (<fields>)] <.func> [as <name>]",
    func: [
      { value: "count([<fields>])" },
      { value: "first(<field>) [offset <offset>]" },
      { value: "last(<field>) [offset <offset>]" },
      { value: "max(<fields>)" },
      { value: "min(<fields>)" },
      { value: "sum(<fields>)" },
    ],
  },
  { name: "union", syntax: "union (<query>)" },
  { name: "uniq", syntax: "uniq [by] (<field>)" },
  { name: "unpack_json", syntax: "unpack_json [from <field>] [result_prefix <prefix>]" },
  { name: "unpack_logfmt", syntax: "unpack_logfmt [from <field>] [result_prefix <prefix>]" },
  { name: "unpack_syslog", syntax: "unpack_syslog [from <field>] [offset <offset>] [result_prefix <prefix>]" },
  { name: "unpack_words", syntax: "unpack_words [from <src>] [as <dst>] [drop_duplicates]" },
  { name: "unroll", syntax: "unroll [by] (<field>)" },
];

const parseSlot = (value: string): PipeSlot => {
  const raw = value.trim();
  return {
    type: "slot",
    name: raw.startsWith(".") ? raw.slice(1) : raw,
    special: raw.startsWith("."),
  };
};

const parseValue = (value: string, start = 0, end = value.length): PipeNode[] => {
  const result: PipeNode[] = [];
  let cursor = start;
  let text = "";

  while (cursor < end) {
    const char = value[cursor];

    if (char === "[") {
      let prefix = "";
      if (text.endsWith(" ")) {
        prefix = " ";
        text = text.slice(0, -1);
      }

      if (text) {
        result.push({ type: "text", value: text });
        text = "";
      }

      let depth = 1;
      let next = cursor + 1;
      for (; next < end; next++) {
        if (value[next] === "[") depth++;
        if (value[next] === "]") {
          depth--;
          if (depth === 0) break;
        }
      }

      result.push({
        type: "optional",
        value: prefix ? [{ type: "text", value: prefix }, ...parseValue(value, cursor + 1, next)] : parseValue(value, cursor + 1, next),
      });
      cursor = next + 1;
      continue;
    }

    if (char === "<") {
      if (text) {
        result.push({ type: "text", value: text });
        text = "";
      }

      const next = value.indexOf(">", cursor + 1);
      result.push(parseSlot(value.slice(cursor + 1, next)));
      cursor = next + 1;
      continue;
    }

    text += char;
    cursor++;
  }

  if (text) {
    result.push({ type: "text", value: text });
  }

  return result;
};

const createPipe = (row: PipeRow): PipeItem => {
  const item: PipeItem = {
    ...row,
    value: parseValue(row.syntax),
  };

  for (const [key, value] of Object.entries(row)) {
    if (!Array.isArray(value)) {
      continue;
    }
    item[key] = value.map((option) => ({
      ...option,
      nodes: parseValue(option.value),
    }));
  }

  return {
    ...item,
  };
};

export const pipeSyntax = pipeRows.map(createPipe);
export const pipeNames = pipeSyntax.map((item) => item.name);

export const getPipe = (name: string): PipeItem | undefined => {
  return pipeSyntax.find((item) => item.name === name);
};

export const getSlotOptions = (pipe: PipeItem | undefined, slotName: string): PipeOption[] => {
  const value = pipe?.[slotName];
  return Array.isArray(value) ? (value as PipeOption[]) : [];
};
