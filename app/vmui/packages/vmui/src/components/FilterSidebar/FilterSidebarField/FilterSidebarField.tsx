import { FC, useEffect, useMemo, useRef, useState } from "preact/compat";
import { LogsFieldValues } from "../../../api/types";
import { formatNumberShort } from "../../../utils/number";
import { ArrowDownIcon, MinusIcon } from "../../Main/Icons";
import "./style.scss";
import classNames from "classnames";
import { useFetchStreamValues } from "../../../pages/OverviewPage/hooks/useFetchStreamValues";
import { useTimeState } from "../../../state/time/TimeStateContext";
import LineLoader from "../../Main/LineLoader/LineLoader";
import { OrderDir } from "../../../types";
import { ExtraFilter, ExtraFilterOperator } from "../../ExtraFilters/types";
import FilterSidebarAlert from "../FilterSidebarAlert/FilterSidebarAlert";
import FilterSidebarValue from "../FilterSidebarValue/FilterSidebarValue";
import Checkbox from "../../Main/Checkbox/Checkbox";
import { useAppState } from "../../../state/common/StateContext";
import Tooltip from "../../Main/Tooltip/Tooltip";
import { buildExtraFilterParams } from "../../ExtraFilters/utils/buildExtraFilterParams";

type Props = {
  field: LogsFieldValues;
  query?: string;
  extraFilters: ExtraFilter[];
  orderDir?: OrderDir;
  onAddFilter: (filter: ExtraFilter) => void;
  onRemoveByValue: (field: string, value: string) => void;
  onRemoveByField: (field: string) => void;
}

const FilterSidebarField: FC<Props> = ({
  field,
  query,
  extraFilters,
  orderDir,
  onAddFilter,
  onRemoveByValue,
  onRemoveByField,
}) => {
  const { isDarkTheme } = useAppState();
  const { period } = useTimeState();
  const [isValuesOpen, setValuesOpen] = useState(false);
  const { fetchStreamValues, streamValues, loading, error, abort } = useFetchStreamValues();

  const extraParams = useMemo(() => {
    const filtersWithoutCurrentField = extraFilters.filter(f => f.field !== field.value);
    return buildExtraFilterParams(filtersWithoutCurrentField);
  }, [field.value, extraFilters]);

  const filtersByName = useMemo(() => {
    return extraFilters.filter(f => f.field === field.value); // for field name
  }, [field.value, extraFilters]);

  const missingSelectedValues: LogsFieldValues[] = useMemo(() => {
    const missingValues = filtersByName.filter(f => {
      return !streamValues.some(v => v.value === f.value);
    });

    return missingValues.map(f => ({ value: f.value, hits: 0 }));
  }, [streamValues, filtersByName]);

  const values = useMemo(() => {
    const allValues = [...streamValues, ...missingSelectedValues];
    if (!orderDir || orderDir === "desc") return allValues;
    return allValues.toSorted((a, b) => a.hits - b.hits);
  }, [streamValues, orderDir, missingSelectedValues]);

  const requestRef = useRef({
    query,
    start: period.start,
    end: period.end,
    extraParams,
  });

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
    if (!streamValues.length) return false;
    const allValues = streamValues.map(v => v.value);
    const checkedValues = filtersByName.map(f => f.value);
    return allValues.every(v => checkedValues.includes(v));
  }, [streamValues, filtersByName]);

  const handleToggleValues = () => {
    setValuesOpen(prev => !prev);
  };

  const handleClickCheckbox = (e: MouseEvent) => {
    e.stopPropagation();

    if (hasFilter) {
      onRemoveByField(field.value);
      return;
    }

    onAddFilter({
      field: field.value,
      value: "",
      operator: ExtraFilterOperator.NotEquals,
      isStream: true,
    });
  };

  const badgeTooltip = useMemo(() => {
    if (hasAnyValueFilter) return "Any non-empty value";
    if (isCheckedAll) return "All listed values";
    return `${filtersByName.length} selected value${filtersByName.length > 1 ? "s" : ""}`;
  }, [hasAnyValueFilter, isCheckedAll, filtersByName.length]);

  useEffect(() => {
    requestRef.current = {
      query,
      start: period.start,
      end: period.end,
      extraParams,
    };
  }, [query, period.start, period.end, extraParams]);

  useEffect(() => {
    // Update only when field changes.
    abort();
    if (!isValuesOpen) return;

    const r = requestRef.current;

    void fetchStreamValues({
      query: r.query,
      field: field.value,
      start: r.start,
      end: r.end,
      extraParams: r.extraParams,
    });

    return abort;
  }, [isValuesOpen, field]);


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
      })}
    >
      {loading && <LineLoader/>}

      <div
        className="vm-filter-sidebar-field-header"
        onClick={handleToggleValues}
      >
        <div
          className="vm-filter-sidebar-field__checkbox"
          onClick={handleClickCheckbox}
        >
          <Checkbox
            icon={hasFilter && !isCheckedAll && !hasAnyValueFilter && <MinusIcon/>}
            size="small"
            color={hasFilter ? (isDarkTheme ? "secondary" : "primary") : "gray"}
            checked={hasFilter}
          />
        </div>

        <div className="vm-filter-sidebar-field-label">
          <span className="vm-filter-sidebar-field-label__title">{field.value}</span>
          <span className="vm-filter-sidebar-field-label__hits">{" "}({hitsShort})</span>
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
        <div className="vm-filter-sidebar-field-values">
          <FilterSidebarAlert
            isVisible={!!error}
            variant="error"
            title="Failed to load stream values"
            message={error}
          />
          {values.map(streamFieldValue => (
            <FilterSidebarValue
              fieldName={field.value}
              key={streamFieldValue.value}
              field={streamFieldValue}
              extraFilters={filtersByName}
              isAnyValueFilter={hasAnyValueFilter}
              onAddFilter={onAddFilter}
              onRemoveByValue={onRemoveByValue}
            />
          ))}
        </div>
      )}
    </div>
  )
  ;
};

export default FilterSidebarField;
