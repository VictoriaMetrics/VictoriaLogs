import { FC, useMemo, useRef } from "preact/compat";
import { useFilterSidebarSticky } from "./hooks/useFilterSidebarSticky";
import { LogsFieldValues } from "../../api/types";
import LineLoader from "../Main/LineLoader/LineLoader";
import FilterSidebarField from "./FilterSidebarField/FilterSidebarField";
import "../Table/TableSettings/style.scss";
import "./style.scss";
import DragResizeHandle from "../Main/DragResizeHandle/DragResizeHandle";
import { useFilterSidebarWidth } from "./hooks/useFilterSidebarWidth";
import { CSSProperties } from "preact";
import FilterSidebarActions from "./FilterSidebarActions/FilterSidebarActions";
import { useFilterSidebarVisible } from "./hooks/useFilterSidebarVisible";
import classNames from "classnames";
import useBoolean from "../../hooks/useBoolean";
import { ExtraFilter } from "../ExtraFilters/types";
import FilterSidebarAlert from "./FilterSidebarAlert/FilterSidebarAlert";
import useDeviceDetect from "../../hooks/useDeviceDetect";

type Props = {
  query: string;
  streamFieldNames: LogsFieldValues[];
  loading: boolean;
  error: string | Error;
  extraFilters: ExtraFilter[];
  onAddFilter: (filter: ExtraFilter) => void;
  onRemoveByValue: (field: string, value: string) => void;
  onRemoveByField: (field: string) => void;
}

const FilterSidebar: FC<Props> = ({
  query,
  streamFieldNames,
  loading,
  error,
  extraFilters,
  onAddFilter,
  onRemoveByValue,
  onRemoveByField,
}) => {
  const { isMobile } = useDeviceDetect();

  const sidebarRef = useRef<HTMLElement>(null);
  const { height, top } = useFilterSidebarSticky(sidebarRef);
  const { size: parentSize, width, setWidth, clearWidth } = useFilterSidebarWidth(sidebarRef);
  const { isVisible, setHidden } = useFilterSidebarVisible();

  const { value: isDescOrder, toggle: toggleSortOrder } = useBoolean(true);
  const orderDir = isDescOrder ? "desc" : "asc";

  const fields = useMemo(() => {
    return isDescOrder
      ? streamFieldNames
      : streamFieldNames.toSorted((a, b) => a.hits - b.hits);
  }, [streamFieldNames, isDescOrder]);

  const sidebarStyles: CSSProperties = useMemo(() => {
    const styles: CSSProperties = { top };
    if (width) styles.width = width;
    if (height) styles.height = height;
    return styles;
  }, [height, top, width]);

  return (
    <section
      className={classNames({
        "vm-filter-sidebar": true,
        "vm-filter-sidebar_hidden": !isVisible,
        "vm-filter-sidebar_mobile": isMobile,
      })}
      style={isMobile ? {} : sidebarStyles}
      ref={sidebarRef}
    >
      {loading && <LineLoader/>}

      <div className="vm-filter-sidebar-header vm-table-settings-section-header">
        <div className="vm-table-settings-section-header__title">Stream fields</div>
        <FilterSidebarActions
          onToggleSort={toggleSortOrder}
          onResetWidth={clearWidth}
          onClose={setHidden}
        />
      </div>

      <FilterSidebarAlert
        isVisible={!!error}
        variant="error"
        title="Failed to load stream fields"
        message={error}
      />

      <FilterSidebarAlert
        isVisible={!error && !loading && fields.length === 0}
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
            orderDir={orderDir}
            onAddFilter={onAddFilter}
            onRemoveByValue={onRemoveByValue}
            onRemoveByField={onRemoveByField}
          />
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
