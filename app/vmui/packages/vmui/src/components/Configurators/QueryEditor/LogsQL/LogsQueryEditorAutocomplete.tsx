import { FC } from "preact/compat";
import Autocomplete from "../../../Main/Autocomplete/Autocomplete";
import { AUTOCOMPLETE_LIMITS } from "../../../../constants/queryAutocomplete";
import { QueryEditorAutocompleteProps } from "../QueryEditor";
import { useExtraFilters } from "../../../ExtraFilters/hooks/useExtraFilters";
import { useLogsQLContext } from "./hooks/useLogsQLContext";
import { useLogsQLFetchOptions } from "./hooks/useLogsQLFetchOptions";
import { useLogsQLOptions } from "./hooks/useLogsQLOptions";
import { useLogsQLSelect } from "./hooks/useLogsQLSelect";
import { DEFAULT_QUERY } from "../../../../pages/QueryPage/hooks/useQueryController";

const LogsQueryEditorAutocomplete: FC<QueryEditorAutocompleteProps> = ({
  value,
  anchorEl,
  caretPosition,
  onSelect,
}) => {
  const { extraParams } = useExtraFilters();

  const { logicalParts, contextData, } = useLogsQLContext({ value, caretPosition });

  const { fieldNames, fieldValues, loading } = useLogsQLFetchOptions({ contextData, extraParams });

  const options = useLogsQLOptions({
    value,
    contextData,
    fieldNames,
    fieldValues,
    showHistory: true,
    showExamples: value === DEFAULT_QUERY || !value.trim(),
  });

  const handleSelect = useLogsQLSelect({
    contextData,
    logicalParts,
    onSelect,
  });

  return (
    <Autocomplete
      value={value === DEFAULT_QUERY ? "" : contextData?.valueContext || ""}
      options={options}
      loading={loading}
      disabledFullScreen
      showKeyboardHints
      anchor={anchorEl}
      fullWidth
      minLength={0}
      offset={{ top: 0, left: 0 }}
      onSelect={handleSelect}
      maxDisplayResults={{
        limit: AUTOCOMPLETE_LIMITS.displayResults,
        message: "Please, specify the query more precisely.",
      }}
    />
  );
};

export default LogsQueryEditorAutocomplete;
