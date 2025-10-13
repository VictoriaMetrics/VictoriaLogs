import { FC, useEffect, useState } from "preact/compat";
import { DeleteIcon, FilterIcon, FilterOffIcon } from "../../../components/Main/Icons";
import { UrlFieldFilterParam, useFieldFilter, useStreamFieldFilter } from "../hooks/useFieldFilter";
import { ExtraFilterOperator } from "./types";
import Tooltip from "../../../components/Main/Tooltip/Tooltip";
import FiltersBarItem from "./FiltersBarItem/FiltersBarItem";
import Button from "../../../components/Main/Button/Button";
import { useSearchParams } from "react-router-dom";
import { useExtraFilters } from "../hooks/useExtraFilters";

const FiltersBarPreview: FC = () => {
  const [, setSearchParams] = useSearchParams();
  const { extraFilters, upsertFilters } = useExtraFilters();
  const { fieldFilter, fieldValueFilters, setFieldFilter, toggleFieldValueFilter } = useFieldFilter();
  const { streamFieldFilter, streamFieldValueFilters, setStreamFieldFilter, toggleStreamFieldValueFilter } = useStreamFieldFilter();

  const fieldValues = fieldValueFilters.length ? fieldValueFilters : ["*"];
  const streamValues = streamFieldValueFilters.length ? streamFieldValueFilters : ["*"];

  const [hasAppliedFilters, setHasAppliedFilter] = useState(false);

  const handleRemoveFieldFilter = (v: string) => () => {
    v === "*" ? setFieldFilter() : toggleFieldValueFilter(v);
  };

  const handleRemoveStreamFilter = (v: string) => () => {
    v === "*" ? setStreamFieldFilter() : toggleStreamFieldValueFilter(v);
  };

  const handleClear = () => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      Object.values(UrlFieldFilterParam).forEach(v => next.delete(v));
      return next;
    });
  };

  const handleApplyFilters = (isInclude: boolean) => () => {
    const operator = isInclude ? ExtraFilterOperator.Equals : ExtraFilterOperator.NotEquals;
    const extraFiltersFields = fieldFilter ? fieldValues.map(v => ({ field: fieldFilter, value: v, operator })) : [];
    const extraFiltersStreams = streamFieldFilter ? streamValues.map(v => ({ field: streamFieldFilter, value: v, operator })) : [];
    const newExtraFilters = extraFiltersFields.concat(extraFiltersStreams);
    upsertFilters(newExtraFilters);
    setHasAppliedFilter(true);
  };

  useEffect(() => {
    if (hasAppliedFilters) {
      handleClear();
      setHasAppliedFilter(false);
    }
  }, [extraFilters]);

  if (!fieldFilter && !streamFieldFilter) return null;

  return (
    <div className="vm-filters-bar vm-filters-bar_preview">
      <div className="vm-filters-bar-title">
        <h2 className="vm-title">Preview filters:</h2>
      </div>

      {fieldFilter && fieldValues.map(v => (
        <Tooltip
          title={"Focus: preview logs only. Doesn’t change Global filters."}
          key={v}
        >
          <FiltersBarItem
            isFocusable
            key={v}
            filter={{ field: fieldFilter, value: v, operator: ExtraFilterOperator.Equals }}
            onRemove={handleRemoveFieldFilter(v)}
          />
        </Tooltip>
      ))}

      {streamFieldFilter && streamValues.map(v => (
        <Tooltip
          title={"Stream focus: preview logs only. Doesn’t change Global filters."}
          key={v}
        >
          <FiltersBarItem
            isFocusable
            key={v}
            filter={{ field: streamFieldFilter, value: v, operator: ExtraFilterOperator.Equals }}
            onRemove={handleRemoveStreamFilter(v)}
          />
        </Tooltip>
      ))}

      <div className="vm-filters-bar__actions">
        <Button
          variant="outlined"
          color="primary"
          onClick={handleApplyFilters(true)}
          startIcon={<FilterIcon/>}
        >
          Include all
        </Button>
        <Button
          variant="outlined"
          color="primary"
          onClick={handleApplyFilters(false)}
          startIcon={<FilterOffIcon/>}
        >
          Exclude all
        </Button>
        <Button
          variant="outlined"
          color="error"
          onClick={handleClear}
          startIcon={<DeleteIcon/>}
        >
          Clear filters
        </Button>
      </div>

      <div className="vm-filters-bar__info">
        These filters affect preview logs only.
        To apply them to Totals, Hits, and Fields/Streams, use <b>Include All</b> or <b>Exclude All</b>.
      </div>
    </div>
  );
};

export default FiltersBarPreview;
