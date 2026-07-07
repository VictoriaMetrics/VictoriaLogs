import { AutocompleteOptions } from "../../../../Main/Autocomplete/Autocomplete";
import { SuggestQuickStartIcon } from "../../../../Main/Icons";
import { ContextType } from "../../LogsQL/types";

export const quickStartExamples: AutocompleteOptions[] = [
  {
    value: "*",
    meta: "Match all entries",
  },
  {
    value: "error",
    meta: "Match word",
  },
  {
    value: "err*",
    meta: "Match word prefix",
  },
  {
    value: "(warn OR error OR fatal)",
    meta: "Match any of the words warn, error or fatal",
  },
  {
    value: "connection AND refused",
    meta: "Match only if both words are present",
  },
  {
    value: "\"connection refused\"",
    meta: "Match phrase",
  },
  {
    value: "error !\"connection refused\"",
    meta: "Match word if phrase is not present",
  },
  {
    value: "level:in(\"error\", \"warn\", \"fatal\")",
    meta: "Match exact field values",
  },
  {
    value: "trace_id:*",
    meta: "Match non-empty field",
  },
  {
    value: "trace_id:* | fields trace_id, span_id",
    meta: "Keep only selected fields",
  },
  {
    value: "error | stats by (service.name) count()",
    meta: "Count errors by service name",
  },
].map(item => ({
  ...item,
  group: "Quick start",
  type: ContextType.Example,
  icon: <SuggestQuickStartIcon />,
}));
