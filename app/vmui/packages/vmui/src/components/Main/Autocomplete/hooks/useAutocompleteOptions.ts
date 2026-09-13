import { useEffect, useMemo } from "preact/compat";
import { AutocompleteOptions } from "../Autocomplete";

type UseAutocompleteOptionsArgs = {
  value: string;
  options: AutocompleteOptions[];
  openAutocomplete: boolean;
  maxDisplayResults?: {
    limit: number;
    message?: string;
  };
  onFoundOptions?: (val: AutocompleteOptions[]) => void;
};

export const useAutocompleteOptions = ({
  value,
  options,
  openAutocomplete,
  maxDisplayResults,
  onFoundOptions,
}: UseAutocompleteOptionsArgs) => {
  const search = value.trim().toLowerCase();

  const filteredOptions = useMemo(() => {
    if (!openAutocomplete) return [];

    if (!search) return options;

    return options.filter((item) => item.value.toLowerCase().includes(search));
  }, [options, openAutocomplete, search]);

  const totalFound = filteredOptions.length;

  const foundOptions = useMemo(() => {
    const end = Math.min(maxDisplayResults?.limit || Infinity, filteredOptions.length);
    const limited = filteredOptions.slice(0, end);
    const isSingleValue = limited.length === 1 &&  limited[0]?.value.toLowerCase() === search;
    return isSingleValue ? [] : limited;
  }, [filteredOptions, maxDisplayResults?.limit, search]);

  const maxResultsMessage = maxDisplayResults?.limit && totalFound > maxDisplayResults.limit
    ? maxDisplayResults.message || ""
    : "";

  const warningMessage = maxResultsMessage
    ? `Shown ${maxDisplayResults?.limit} results out of ${totalFound}. ${maxResultsMessage}`
    : "";

  useEffect(() => {
    onFoundOptions?.(foundOptions);
  }, [foundOptions, onFoundOptions]);

  return {
    foundOptions,
    warningMessage,
  };
};
