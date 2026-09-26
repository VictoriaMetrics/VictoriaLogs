import { useMemo, useState } from "preact/compat";
import { AutocompleteOptions } from "../../../../Main/Autocomplete/Autocomplete";
import { ContextType, ContextData } from "../types";
import { pipeOptions } from "../helpers/pipes";
import { getHistoryFromStorage } from "../../../../QueryHistory/utils";
import useEventListener from "../../../../../hooks/useEventListener";
import { vmDate } from "../../../../../utils/time";
import { HistoryIcon } from "../../../../Main/Icons";
import { quickStartExamples } from "../../QueryExamples/examples/quickStartExamples";
import { DEFAULT_QUERY } from "../../../../../pages/QueryPage/hooks/useQueryController";

type UseLogsQLOptionsArgs = {
  value: string;
  contextData?: ContextData;
  fieldNames: AutocompleteOptions[];
  fieldValues: AutocompleteOptions[];
  showHistory: boolean;
  showExamples: boolean;
};

export const useLogsQLOptions = ({
  value,
  contextData,
  fieldNames,
  fieldValues,
  showHistory,
  showExamples,
}: UseLogsQLOptionsArgs): AutocompleteOptions[] => {
  const baseOptions: AutocompleteOptions[] = useMemo(() => {
    const isDefaultQuery = contextData?.query === DEFAULT_QUERY || !contextData?.query;
    if (isDefaultQuery) return [];

    switch (contextData?.contextType) {
      case ContextType.FilterName:
      case ContextType.FilterUnknown:
        return fieldNames;

      case ContextType.FilterValue:
        return fieldValues;

      case ContextType.PipeName:
        return pipeOptions;

      case ContextType.FilterOrPipeName:
        return [...fieldNames, ...pipeOptions];

      default:
        return [];
    }
  }, [contextData, fieldNames, fieldValues]);

  const [historyStorage, setHistoryStorage] = useState(() => getHistoryFromStorage());

  const historyOptions: AutocompleteOptions[] = useMemo(() => {
    return historyStorage
      .toSorted((a,b) => (b.lastRunAt ?? -Infinity) - (a.lastRunAt ?? -Infinity))
      .map(item => ({
        value: item.query,
        type: ContextType.History,
        group: "History",
        meta: item.lastRunAt ? vmDate(item.lastRunAt).fromNow(true) : "earlier",
        icon: <HistoryIcon/>
      }));
  }, [historyStorage]);

  const updateStageHistory = () => {
    setHistoryStorage(getHistoryFromStorage());
  };

  useEventListener("storage", updateStageHistory);

  return useMemo(() => {
    const sections = [baseOptions];

    if (showHistory) {
      const historySection = historyOptions.filter(o => o.value !== value);
      sections.push(historySection);
    }

    if (showExamples) {
      const examplesSection = quickStartExamples.filter(o => o.value !== value);
      sections.unshift(examplesSection);
    }

    return sections.flat();
  }, [value, baseOptions, historyOptions, showHistory, showExamples]);
};
