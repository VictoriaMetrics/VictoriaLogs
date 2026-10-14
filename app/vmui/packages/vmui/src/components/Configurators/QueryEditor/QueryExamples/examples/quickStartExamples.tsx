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
    meta: "Match prefix",
  },
  {
    value: "\"connection refused\"",
    meta: "Match phrase",
  },
  {
    value: "error !(\"connection refused\" OR reset)",
    meta: "Match word error, but exclude phrases or words in parentheses",
  },
  {
    value: "level:in(\"error\", \"warn\", \"fatal\")",
    meta: "Match exact field values",
  },
  {
    value: "trace_id:*",
    meta: "Match entries with non-empty field",
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
