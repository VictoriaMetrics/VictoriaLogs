import { FC, Fragment, useCallback, useEffect, useMemo, useState } from "preact/compat";
import { LogsFieldValues } from "../../../api/types";
import { formatNumber, formatNumberShort } from "../../../utils/number";
import { ArrowDownIcon, SearchIcon } from "../../Main/Icons";
import "./style.scss";
import classNames from "classnames";
import { useFetchStreamValues } from "../../../pages/OverviewPage/hooks/useFetchStreamValues";
import LineLoader from "../../Main/LineLoader/LineLoader";
import { ExtraFilter, ExtraFilterOperator } from "../../ExtraFilters/types";
import FilterSidebarAlert from "../FilterSidebarAlert/FilterSidebarAlert";
import FilterSidebarValue from "../FilterSidebarValue/FilterSidebarValue";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { buildExtraFilterParams } from "../../ExtraFilters/utils/buildExtraFilterParams";
import { sortSidebarItems } from "../utils/sortSidebarItems";
import { filterSidebarItemsBySearch } from "../utils/filterSidebarItems";
import { isStreamFilter } from "../../ExtraFilters/utils/isStreamFilter";
import { SIDEBAR_ITEMS_LIMIT, SIDEBAR_SEARCH_THRESHOLD } from "../constants";
import TextField, { TextFieldKeyboardEvent } from "../../Main/TextField/TextField";
import { useDebounceCallback } from "../../../hooks/useDebounceCallback";
import FilterSidebarSort from "../FilterSidebarSort/FilterSidebarSort";
import { useFilterSidebarSort } from "../hooks/useFilterSidebarSort";
import { useQueryState } from "../../../state/query/QueryStateContext";

const ANY_VALUE_OPTION: LogsFieldValues = {
  value: "",
  hits: 0,
};

const ANY_VALUE_LABEL = "Any value";

type Props = {
  field: LogsFieldValues;
  query?: string;
  extraFilters: ExtraFilter[];
  onAddFilter: (filter: ExtraFilter) => void;
  onRemoveByValue: (field: string, value: string) => void;
  onReplaceFiltersByField: (filter: ExtraFilter) => void;
}

const FilterSidebarField: FC<Props> = ({
  field,
  query,
  extraFilters,
  onAddFilter,
  onRemoveByValue,
  onReplaceFiltersByField,
}) => {
  const { appliedPeriod } = useQueryState();
  const { fetchStreamValues, streamValues, loading, error, abort } = useFetchStreamValues();
  const [isServerSearch, setIsServerSearch] = useState(false);
  const isReady = !loading && !error;

  const [isValuesOpen, setValuesOpen] = useState(false);
  const [search, setSearch] = useState("");

  const { sort } = useFilterSidebarSort();

  const extraParams = useMemo(() => {
    const filtersWithoutCurrentField = extraFilters.filter(f => f.field !== field.value);
    return buildExtraFilterParams(filtersWithoutCurrentField);
  }, [field.value, extraFilters]);

  const filtersByName = useMemo(() => {
    return extraFilters.filter(f => isStreamFilter(f) && (f.field === field.value)); // for field name
  }, [field.value, extraFilters]);

  const missingSelectedValues: LogsFieldValues[] = useMemo(() => {
    const missingValues = filtersByName.filter(f => {
      return f.value !== "" && !streamValues.some(v => v.value === f.value);
    });

    const uniqMissingValues = [...new Set(missingValues.map(f => f.value))];

    return uniqMissingValues.map(value => ({ value, hits: 0 }));
  }, [streamValues, filtersByName]);

  const allValues = useMemo(() => {
    return [...streamValues, ...missingSelectedValues];
  }, [streamValues, missingSelectedValues]);

  const isLimitExceeded = streamValues.length > SIDEBAR_ITEMS_LIMIT;
  const showInputSearch = isServerSearch || allValues.length > SIDEBAR_SEARCH_THRESHOLD.value || search.length > 0;

  const selectedFilters = useMemo(() => {
    const filtered = filtersByName.filter(filter => filter.value !== "");
    return new Set(filtered.map(filter => filter.value));
  }, [filtersByName]);

  const values = useMemo(() => {
    if (isLimitExceeded) {
      return search.trim()
        ? []
        : sortSidebarItems(
          allValues.filter(v => selectedFilters.has(v.value)),
          sort,
          selectedFilters,
        );
    }

    return isServerSearch
      ? sortSidebarItems(streamValues, sort, selectedFilters)
      : filterSidebarItemsBySearch(sortSidebarItems(allValues, sort, selectedFilters), search);
  }, [allValues, search, isServerSearch, streamValues, selectedFilters, sort, isLimitExceeded]);

  const firstAvailableIndex = values.findIndex(v => !selectedFilters.has(v.value));
  const selectedCount = firstAvailableIndex === -1 ? values.length : firstAvailableIndex;
  const availableCount = values.length - selectedCount;

  const hasFilter = !!filtersByName.length;
  const hitsShort = formatNumberShort(field.hits);

  const hasAnyValueFilter = useMemo(() => {
    return filtersByName.some(f => {
      const isNegate = f.operator === ExtraFilterOperator.NotEquals;
      const isAllValues = f.value === "";
      return isNegate && isAllValues;
    });
  }, [filtersByName]);

  const isCheckedAll = useMemo(() => {
    if (isServerSearch || isLimitExceeded || !streamValues.length) return false;

    const checkedValues = filtersByName.map(f => f.value);
    return streamValues.every(v => checkedValues.includes(v.value));
  }, [streamValues, filtersByName, isServerSearch, isLimitExceeded]);

  const handleToggleValues = () => {
    setValuesOpen(value => !value);
  };

  const handleSelectAnyValue = () => {
    if (hasAnyValueFilter) {
      onRemoveByValue(field.value, "");
      return;
    }

    onReplaceFiltersByField({
      field: field.value,
      value: "",
      operator: ExtraFilterOperator.NotEquals,
      isStream: true,
    });
  };

  const handleSelectValue = (filter: ExtraFilter) => {
    if (hasAnyValueFilter) {
      onReplaceFiltersByField(filter);
      return;
    }

    onAddFilter(filter);
  };

  const handleSearchKeyDown = (event: TextFieldKeyboardEvent) => {
    if (event.key === "Escape") setSearch("");
  };

  const badgeTooltip = useMemo(() => {
    if (hasAnyValueFilter) return "Any non-empty value";
    if (isCheckedAll) return "All listed values";
    return `${filtersByName.length} selected value${filtersByName.length > 1 ? "s" : ""}`;
  }, [hasAnyValueFilter, isCheckedAll, filtersByName.length]);

  const fetchValues = useCallback(async (filter?: string) => {
    if (!appliedPeriod) return;

    const data = await fetchStreamValues({
      query,
      field: field.value,
      period: appliedPeriod,
      extraParams,
      filter,
    });
    if (data === undefined) return;
    if (!filter?.trim()) setIsServerSearch(data.length > SIDEBAR_ITEMS_LIMIT);
  }, [fetchStreamValues, query, field.value, appliedPeriod, extraParams.toString()]);

  const debouncedFetchValues = useDebounceCallback(fetchValues, 300);
  const debouncedSearchValues = useDebounceCallback(fetchValues, 300);

  useEffect(() => {
    setIsServerSearch(false);
    if (!isValuesOpen) return;

    debouncedFetchValues();

    return () => {
      debouncedFetchValues.cancel();
      abort();
    };
  }, [isValuesOpen, fetchValues, abort]);

  useEffect(() => {
    if (!isValuesOpen || !isServerSearch) return;

    debouncedSearchValues(search.trim());

    return () => {
      debouncedSearchValues.cancel();
      abort();
    };
  }, [isValuesOpen, search, isServerSearch, abort]);

  useEffect(() => {
    // Only for the first render.
    if (!hasFilter) return;
    setValuesOpen(true);
  }, []);

  return (
    <div
      className={classNames({
        "vm-filter-sidebar-field": true,
        "vm-filter-sidebar-field_open": isValuesOpen,
        "vm-filter-sidebar-field_active": hasFilter,
        "vm-filter-sidebar-field_empty": !field.hits
      })}
    >
      {loading && <LineLoader/>}

      <div
        className="vm-filter-sidebar-field-header"
        onClick={handleToggleValues}
      >
        <div className="vm-filter-sidebar-field-label">
          <span className="vm-filter-sidebar-field-label__title">{field.value}</span>
          <Tooltip
            placement="top-left"
            title="Number of matching logs containing this field"
          >
            <span className="vm-filter-sidebar-field-label__hits">{" "}({hitsShort})</span>
          </Tooltip>
        </div>

        {hasFilter && (
          <Tooltip
            placement="top-center"
            title={badgeTooltip}
          >
            <div className="vm-filter-sidebar-field__badge">
              {hasAnyValueFilter && "Any"}
              {isCheckedAll && !hasAnyValueFilter && "All"}
              {!isCheckedAll && !hasAnyValueFilter && filtersByName.length}
            </div>
          </Tooltip>
        )}

        <div
          className={classNames({
            "vm-filter-sidebar-field__arrow": true,
            "vm-filter-sidebar-field__arrow_open": isValuesOpen,
          })}
        >
          <ArrowDownIcon/>
        </div>

      </div>

      {isValuesOpen && (
        <>
          <FilterSidebarAlert
            isVisible={!!error}
            variant="error"
            title="Failed to load stream values"
            message={error}
          />

          <FilterSidebarAlert
            isVisible={isReady && !isLimitExceeded && !!search.trim() && !values.length}
            variant="info"
            title="No values found"
          />

          <div className="vm-filter-sidebar-field-values__header">
            <FilterSidebarValue
              isAnyValueFilter
              checked={hasAnyValueFilter}
              fieldName={field.value}
              key="any-value-option"
              label={ANY_VALUE_LABEL}
              field={ANY_VALUE_OPTION}
              extraFilters={filtersByName}
              onAddFilter={handleSelectAnyValue}
              onRemoveByValue={handleSelectAnyValue}
            />

            <FilterSidebarSort/>
          </div>

          {showInputSearch && (
            <div className="vm-filter-sidebar-search vm-filter-sidebar-search_value">
              <TextField
                type="search"
                placeholder="Search field values"
                startIcon={<SearchIcon/>}
                value={search}
                onChange={setSearch}
                onKeyDown={handleSearchKeyDown}
              />
            </div>
          )}

          <div className="vm-filter-sidebar-field-values">
            {values.map((fieldValue, index) => (
              <Fragment key={fieldValue.value}>
                {index === 0 && selectedCount > 0 && (
                  <p className="vm-filter-sidebar-list__title">Selected ({selectedCount})</p>
                )}

                {index === firstAvailableIndex && (
                  <p className="vm-filter-sidebar-list__title">Available ({availableCount})</p>
                )}

                <FilterSidebarValue
                  fieldName={field.value}
                  field={fieldValue}
                  extraFilters={filtersByName}
                  onAddFilter={handleSelectValue}
                  onRemoveByValue={onRemoveByValue}
                />
              </Fragment>
            ))}
          </div>

          <FilterSidebarAlert
            isVisible={isReady && isLimitExceeded}
            variant="warning"
            title={`More than ${formatNumber(SIDEBAR_ITEMS_LIMIT)} field values found`}
            message="Start typing to narrow the search."
          />
        </>
      )}
    </div>
  )
  ;
};

export default FilterSidebarField;
