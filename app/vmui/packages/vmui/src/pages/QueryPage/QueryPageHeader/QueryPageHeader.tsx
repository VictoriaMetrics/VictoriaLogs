import { FC, useRef } from "preact/compat";
import { PlayIcon, SpinnerIcon } from "../../../components/Main/Icons";
import "./style.scss";
import Button from "../../../components/Main/Button/Button";
import QueryEditor from "../../../components/Configurators/QueryEditor/QueryEditor";
import LogsLimitInput from "../../../components/Configurators/LogsLimitController/LogsLimitInput";
import LogsQueryEditorAutocomplete
  from "../../../components/Configurators/QueryEditor/LogsQL/LogsQueryEditorAutocomplete";
import { useQueryState } from "../../../state/query/QueryStateContext";
import QueryHistory from "../../../components/QueryHistory/QueryHistory";
import { useQuickAutocomplete } from "../../../hooks/useQuickAutocomplete";
import FilterSidebarToggle from "../../../components/FilterSidebar/FilterSidebarToggle";
import AutocompleteToggle from "../../../components/Configurators/QueryEditor/AutocompleteToggle";
import ExtraFiltersReset from "../../../components/ExtraFilters/ExtraFiltersPanel/ExtraFiltersReset";
import ExtraFiltersCopy from "../../../components/ExtraFilters/ExtraFiltersPanel/ExtraFiltersCopy";
import QueryExamplesButton from "../../../components/Configurators/QueryEditor/QueryExamples/QueryExamplesButton";
import { getHistoryFromStorage } from "../../../components/QueryHistory/utils";

interface Props {
  query: string;
  queryDurationMs?: number;
  limit: number;
  error?: string;
  isLoading: boolean;
  onChange: (val: string) => void;
  onChangeLimit: (val: number) => void;
  onRun: (query?: string) => void;
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
  const { autocompleteQuick } = useQueryState();
  const setQuickAutocomplete = useQuickAutocomplete();

  const historyIndexRef = useRef<number | null>(null);

  const handleHistoryChange = (step: number) => {
    const history = getHistoryFromStorage();
    if (!history.length) return;

    const currentIndex = historyIndexRef.current ?? (step > 0 ? -1 : 0);
    const nextIndex = currentIndex + step;

    if (nextIndex < 0 || nextIndex >= history.length) return;

    historyIndexRef.current = nextIndex;
    onChange(history[nextIndex].query);
  };

  const createHandlerArrow = (step: number) => () => {
    handleHistoryChange(step);
  };

  const handleChangeAndRun = (value: string) => {
    onChange(value);
    onRun(value);
  };

  const onChangeHandle = (value: string) => {
    historyIndexRef.current = null;
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
          autocompleteEl={LogsQueryEditorAutocomplete}
          onArrowUp={createHandlerArrow(1)}
          onArrowDown={createHandlerArrow(-1)}
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
        <QueryHistory handleSelectQuery={handleChangeAndRun}/>
        <div className="vm-query-page-header-bottom-execute">
          <Button
            startIcon={isLoading ? <SpinnerIcon/> : <PlayIcon/>}
            onClick={() => onRun()}
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
