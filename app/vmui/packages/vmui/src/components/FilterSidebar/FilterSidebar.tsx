import { FC, useEffect, useMemo, useRef, useCallback, useState } from "preact/compat";
import { useFilterSidebarSticky } from "./hooks/useFilterSidebarSticky";
import LineLoader from "../Main/LineLoader/LineLoader";
import FilterSidebarField from "./FilterSidebarField/FilterSidebarField";
import "../Table/TableSettings/style.scss";
import "./style.scss";
import DragResizeHandle from "../Main/DragResizeHandle/DragResizeHandle";
import { useFilterSidebarWidth } from "./hooks/useFilterSidebarWidth";
import { CSSProperties } from "preact";
import FilterSidebarActions from "./FilterSidebarActions/FilterSidebarActions";
import classNames from "classnames";
import { ExtraFilter } from "../ExtraFilters/types";
import FilterSidebarAlert from "./FilterSidebarAlert/FilterSidebarAlert";
import useDeviceDetect from "../../hooks/useDeviceDetect";
import { isStreamFilter } from "../ExtraFilters/utils/isStreamFilter";
import { useFetchStreamFieldNames } from "../../pages/OverviewPage/hooks/useFetchStreamNames";
import { useTimePeriod } from "../../pages/QueryPage/hooks/useTimePeriod";
import { useDebounceCallback } from "../../hooks/useDebounceCallback";
import { LogsFieldValues } from "../../api/types";
import { sortSidebarItems } from "./utils/sortSidebarItems";
import { useFilterSidebarSort } from "./hooks/useFilterSidebarSort";
import TextField, { TextFieldKeyboardEvent } from "../Main/TextField/TextField";
import { SearchIcon } from "../Main/Icons";

type Props = {
  query: string;
  extraFilters: ExtraFilter[];
  extraParams: URLSearchParams;
  onAddFilter: (filter: ExtraFilter) => void;
  onReplaceFiltersByField: (filter: ExtraFilter) => void;
  onRemoveByValue: (field: string, value: string) => void;
  onClose: () => void;
}

const FilterSidebar: FC<Props> = ({
  query,
  extraFilters,
  extraParams,
  onAddFilter,
  onReplaceFiltersByField,
  onRemoveByValue,
  onClose,
}) => {
  const { isMobile } = useDeviceDetect();
  const { getCurrentPeriod } = useTimePeriod();

  const { fetchStreamFieldNames, streamFieldNames, loading, error, abort } = useFetchStreamFieldNames();
  const [isLoaded, setIsLoaded] = useState(false);
  const [search, setSearch] = useState("");

  const sidebarRef = useRef<HTMLElement>(null);
  const { height, top } = useFilterSidebarSticky(sidebarRef);
  const { size: parentSize, width, setWidth, clearWidth } = useFilterSidebarWidth(sidebarRef);

  const { sort, setSort } = useFilterSidebarSort();

  const missingSelectedFields: LogsFieldValues[] = useMemo(() => {
    const missingFields = extraFilters.filter(f =>
      isStreamFilter(f) && !streamFieldNames.some(v => v.value === f.field)
    );

    const uniqMissingFields = [...new Set(missingFields.map(f => f.field))];
    return uniqMissingFields.map(value => ({ value, hits: 0 }));
  }, [streamFieldNames, extraFilters]);

  const fields = useMemo(() => {
    const allFields = [...streamFieldNames, ...missingSelectedFields];
    const selectedFields = new Set(extraFilters.filter(isStreamFilter).map(filter => filter.field));

    return sortSidebarItems(allFields, sort, selectedFields);
  }, [streamFieldNames, missingSelectedFields, extraFilters, sort.by, sort.direction]);

  const handleSearchKeyDown = (event: TextFieldKeyboardEvent) => {
    if (event.key === "Escape") setSearch("");
  };

  const sidebarStyles: CSSProperties | undefined = useMemo(() => {
    if (isMobile) return;

    const styles: CSSProperties = { top };
    if (width) styles.width = width;
    if (height) styles.height = height;
    return styles;
  }, [height, top, width, isMobile]);

  const fetchStreams = useCallback(async () => {
    try {
      const period = getCurrentPeriod();
      await fetchStreamFieldNames({ period, query, extraParams });
      setIsLoaded(true);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") return;
      throw err;
    }
  }, [fetchStreamFieldNames, getCurrentPeriod, query, extraParams.toString()]);

  const debouncedFetchStreams = useDebounceCallback(fetchStreams, 300);

  useEffect(() => {
    debouncedFetchStreams();
    return abort;
  }, [fetchStreams, abort]);

  return (
    <section
      className={classNames({
        "vm-filter-sidebar": true,
        "vm-filter-sidebar_mobile": isMobile,
      })}
      style={sidebarStyles}
      ref={sidebarRef}
    >
      {loading && <LineLoader/>}

      <div className="vm-filter-sidebar-header vm-table-settings-section-header">
        <div className="vm-table-settings-section-header__title">Stream fields</div>
        <FilterSidebarActions
          sort={sort}
          onChangeSort={setSort}
          onResetWidth={clearWidth}
          onClose={onClose}
        />
      </div>

      <div className="vm-filter-sidebar-search">
        <TextField
          type="search"
          placeholder="Search fields and expanded values"
          startIcon={<SearchIcon/>}
          value={search}
          onChange={setSearch}
          onKeyDown={handleSearchKeyDown}
        />
      </div>

      <FilterSidebarAlert
        isVisible={!!error}
        variant="error"
        title="Failed to load stream fields"
        message={error}
      />

      <FilterSidebarAlert
        isVisible={!error && !loading && fields.length === 0 && isLoaded}
        variant="info"
        title="No stream fields found"
      />

      <div className="vm-filter-sidebar-list">
        {fields.map((field) => (
          <FilterSidebarField
            key={field.value}
            query={query}
            field={field}
            extraFilters={extraFilters}
            sort={sort}
            search={search}
            onAddFilter={onAddFilter}
            onRemoveByValue={onRemoveByValue}
            onReplaceFiltersByField={onReplaceFiltersByField}
          />
        ))}
      </div>

      {!!search.trim() && fields.length > 0 && !error && !loading && isLoaded && (
        <div className="vm-filter-sidebar-empty">
          <FilterSidebarAlert
            isVisible
            variant="info"
            title="No fields or values match your search"
          />
        </div>
      )}

      <DragResizeHandle
        targetRef={sidebarRef}
        minSize={250}
        dir={1}
        size={parentSize}
        onResizeEnd={setWidth}
      />
    </section>
  );
};

export default FilterSidebar;
