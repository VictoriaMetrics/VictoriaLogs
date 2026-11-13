import { FC, useEffect, useMemo, useRef, useCallback, useState, Fragment } from "preact/compat";
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
import { useDebounceCallback } from "../../hooks/useDebounceCallback";
import { LogsFieldValues } from "../../api/types";
import { sortSidebarItems } from "./utils/sortSidebarItems";
import TextField, { TextFieldKeyboardEvent } from "../Main/TextField/TextField";
import { SearchIcon } from "../Main/Icons";
import { filterSidebarItemsBySearch } from "./utils/filterSidebarItems";
import { formatNumber } from "../../utils/number";
import { useQueryState } from "../../state/query/QueryStateContext";
import {
  SIDEBAR_ITEMS_LIMIT,
  SIDEBAR_SEARCH_THRESHOLD,
  DEFAULT_FILTER_SIDEBAR_SORT,
} from "./constants";

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
  const { appliedPeriod } = useQueryState();

  const { fetchStreamFieldNames, streamFieldNames, loading, error, abort } = useFetchStreamFieldNames();
  const [isLoaded, setIsLoaded] = useState(false);
  const [isServerSearch, setIsServerSearch] = useState(false);
  const isReady = isLoaded && !loading && !error;

  const [search, setSearch] = useState("");

  const sidebarRef = useRef<HTMLElement>(null);
  const { height, top } = useFilterSidebarSticky(sidebarRef);
  const { size: parentSize, width, setWidth, clearWidth } = useFilterSidebarWidth(sidebarRef);

  const missingSelectedFields: LogsFieldValues[] = useMemo(() => {
    const missingFields = extraFilters.filter(f =>
      isStreamFilter(f) && !streamFieldNames.some(v => v.value === f.field)
    );

    const uniqMissingFields = [...new Set(missingFields.map(f => f.field))];
    return uniqMissingFields.map(value => ({ value, hits: 0 }));
  }, [streamFieldNames, extraFilters]);

  const allFields = useMemo(() => {
    return [...streamFieldNames, ...missingSelectedFields];
  }, [streamFieldNames, missingSelectedFields]);

  const isStreamsLimitExceeded = streamFieldNames.length > SIDEBAR_ITEMS_LIMIT;
  const isSearchRequired = isStreamsLimitExceeded || (isServerSearch && !search.trim());
  const showInputSearch = isServerSearch || allFields.length > SIDEBAR_SEARCH_THRESHOLD.field || search.length > 0;

  const selectedFilters = useMemo(() => {
    return new Set(extraFilters.filter(isStreamFilter).map(filter => filter.field));
  }, [extraFilters]);

  const fields = useMemo(() => {
    if (isSearchRequired) {
      return search.trim()
        ? []
        : sortSidebarItems(
          allFields.filter(f => selectedFilters.has(f.value)),
          DEFAULT_FILTER_SIDEBAR_SORT,
          selectedFilters,
        );
    }

    if (isServerSearch) {
      return sortSidebarItems(streamFieldNames, DEFAULT_FILTER_SIDEBAR_SORT, selectedFilters);
    }

    const sorted = sortSidebarItems(allFields, DEFAULT_FILTER_SIDEBAR_SORT, selectedFilters);
    return filterSidebarItemsBySearch(sorted, search);
  }, [allFields, streamFieldNames, search, isServerSearch, isSearchRequired, selectedFilters]);

  const firstAvailableIndex = fields.findIndex(f => !selectedFilters.has(f.value));
  const selectedCount = firstAvailableIndex === -1 ? fields.length : firstAvailableIndex;
  const availableCount = fields.length - selectedCount;

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

  const fetchStreams = useCallback(async (filter?: string) => {
    if (!appliedPeriod) return;
    const data = await fetchStreamFieldNames({
      period: appliedPeriod,
      query,
      extraParams,
      filter
    });

    if (data === undefined) return;
    setIsLoaded(true);
    if (!filter?.trim()) setIsServerSearch(data.length > SIDEBAR_ITEMS_LIMIT);
  }, [fetchStreamFieldNames, appliedPeriod, query, extraParams.toString()]);

  const debouncedFetchStreams = useDebounceCallback(fetchStreams, 300);
  const debouncedSearchStreams = useDebounceCallback(fetchStreams, 300);

  useEffect(() => {
    setIsServerSearch(false);
    debouncedFetchStreams();

    return () => {
      debouncedFetchStreams.cancel();
      abort();
    };
  }, [fetchStreams, abort]);

  useEffect(() => {
    const filter = search.trim();
    if (!isServerSearch || !filter) return;

    debouncedSearchStreams(filter);

    return () => {
      debouncedSearchStreams.cancel();
      abort();
    };
  }, [search, isServerSearch]);

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
          onResetWidth={clearWidth}
          onClose={onClose}
        />
      </div>

      {showInputSearch && (
        <div className="vm-filter-sidebar-search">
          <TextField
            type="search"
            placeholder="Search field names"
            startIcon={<SearchIcon/>}
            value={search}
            onChange={setSearch}
            onKeyDown={handleSearchKeyDown}
          />
        </div>
      )}

      <FilterSidebarAlert
        isVisible={!!error}
        variant="error"
        title="Failed to load stream fields"
        message={error}
      />

      <FilterSidebarAlert
        isVisible={isReady && !isSearchRequired && !fields.length}
        variant="info"
        title="No fields found"
      />

      <FilterSidebarAlert
        isVisible={isReady && isSearchRequired}
        variant="warning"
        title={`More than ${formatNumber(SIDEBAR_ITEMS_LIMIT)} fields found`}
        message="Start typing to narrow the search."
      />

      <div className="vm-filter-sidebar-list">
        {fields.map((field, index) => (
          <Fragment key={field.value}>
            {index === 0 && selectedCount > 0 && (
              <p className="vm-filter-sidebar-list__title">Selected ({selectedCount})</p>
            )}

            {index === firstAvailableIndex && (
              <p className="vm-filter-sidebar-list__title">Available ({availableCount})</p>
            )}

            <FilterSidebarField
              query={query}
              field={field}
              extraFilters={extraFilters}
              onAddFilter={onAddFilter}
              onRemoveByValue={onRemoveByValue}
              onReplaceFiltersByField={onReplaceFiltersByField}
            />
          </Fragment>
        ))}
      </div>

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
