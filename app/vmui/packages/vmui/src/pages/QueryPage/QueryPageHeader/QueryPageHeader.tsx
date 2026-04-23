import { FC, useEffect } from "preact/compat";
import { PlayIcon, SpinnerIcon } from "../../../components/Main/Icons";
import "./style.scss";
import Button from "../../../components/Main/Button/Button";
import QueryEditor from "../../../components/Configurators/QueryEditor/QueryEditor";
import LogsLimitInput from "../LimitController/LogsLimitInput";
import LogsQueryEditorAutocomplete
  from "../../../components/Configurators/QueryEditor/LogsQL/LogsQueryEditorAutocomplete";
import { useQueryDispatch, useQueryState } from "../../../state/query/QueryStateContext";
import QueryHistory from "../../../components/QueryHistory/QueryHistory";
import useBoolean from "../../../hooks/useBoolean";
import { useQuickAutocomplete } from "../../../hooks/useQuickAutocomplete";
import FilterSidebarToggle from "../../../components/FilterSidebar/FilterSidebarToggle";
import AutocompleteToggle from "../../../components/Configurators/QueryEditor/AutocompleteToggle";
import ExtraFiltersReset from "../../../components/ExtraFilters/ExtraFiltersPanel/ExtraFiltersReset";
import ExtraFiltersCopy from "../../../components/ExtraFilters/ExtraFiltersPanel/ExtraFiltersCopy";
import QueryExamplesButton from "../../../components/Configurators/QueryEditor/QueryExamples/QueryExamplesButton";

interface Props {
  query: string;
  queryDurationMs?: number;
  limit: number;
  error?: string;
  isLoading: boolean;
  onChange: (val: string) => void;
  onChangeLimit: (val: number) => void;
  onRun: () => void;
}

const QueryPageHeader: FC<Props> = ({
  query,
  queryDurationMs,
  limit,
  error,
  isLoading,
  onChange,
  onChangeLimit,
  onRun,
}) => {
  const { autocomplete, queryHistory, autocompleteQuick } = useQueryState();
  const queryDispatch = useQueryDispatch();
  const setQuickAutocomplete = useQuickAutocomplete();

  const { value: awaitQuery, setValue: setAwaitQuery } = useBoolean(false);

  const handleHistoryChange = (step: number) => {
    const { values, index } = queryHistory[0];
    const newIndexHistory = index + step;
    if (newIndexHistory < 0 || newIndexHistory >= values.length) return;
    onChange(values[newIndexHistory] || "");
    queryDispatch({
      type: "SET_QUERY_HISTORY_BY_INDEX",
      payload: { value: { values, index: newIndexHistory }, queryNumber: 0 }
    });
  };

  const handleChangeAndRun = (value: string) => {
    onChange(value);
    setAwaitQuery(true);
  };

  const createHandlerArrow = (step: number) => () => {
    handleHistoryChange(step);
  };

  useEffect(() => {
    if (awaitQuery) {
      onRun();
      setAwaitQuery(false);
    }
  }, [query, awaitQuery]);

  const onChangeHandle = (value: string) => {
    onChange(value);
    if (autocompleteQuick) {
      setQuickAutocomplete(false);
    }
  };

  return (
    <>
      <div className="vm-query-page-header-top">
        <QueryEditor
          value={query}
          autocomplete={autocomplete || autocompleteQuick}
          autocompleteEl={LogsQueryEditorAutocomplete}
          onArrowUp={createHandlerArrow(-1)}
          onArrowDown={createHandlerArrow(1)}
          onEnter={onRun}
          onChange={onChangeHandle}
          label={"Query"}
          error={error}
          stats={{ executionTimeMs: queryDurationMs }}
        />
        <LogsLimitInput
          limit={limit}
          onChangeLimit={onChangeLimit}
          onPressEnter={onRun}
        />
      </div>
      <div className="vm-query-page-header-bottom">
        <div className="vm-query-page-header-bottom-contols">
          <FilterSidebarToggle/>
          <ExtraFiltersReset/>
          <ExtraFiltersCopy/>
        </div>
        <QueryExamplesButton onApply={handleChangeAndRun}/>
        <AutocompleteToggle/>
        <QueryHistory
          handleSelectQuery={handleChangeAndRun}
          historyKey={"LOGS_QUERY_HISTORY"}
        />
        <div className="vm-query-page-header-bottom-execute">
          <Button
            startIcon={isLoading ? <SpinnerIcon/> : <PlayIcon/>}
            onClick={onRun}
            fullWidth
          >
            <div>
              <span className="vm-query-page-header-bottom-execute__text">
                {isLoading ? "Cancel" : "Execute"}
              </span>
              <span className="vm-query-page-header-bottom-execute__text_hidden">Execute</span>
            </div>
          </Button>
        </div>
      </div>
    </>
  );
};

export default QueryPageHeader;
