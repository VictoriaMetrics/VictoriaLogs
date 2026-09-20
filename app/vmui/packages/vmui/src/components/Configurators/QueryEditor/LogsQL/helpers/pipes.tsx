import { ContextType } from "../types";
import { SuggestFunctionIcon } from "../../../../Main/Icons";
import { pipes } from "../../../../../generated/logsql.pipes";
import { AutocompleteOptions } from "../../../../Main/Autocomplete/Autocomplete";

export const pipeOptions: AutocompleteOptions[] = pipes.map(item => ({
  ...item,
  type: ContextType.PipeName,
  group: "Autocomplete",
  meta: "pipe",
  icon: <SuggestFunctionIcon/>,
}));
