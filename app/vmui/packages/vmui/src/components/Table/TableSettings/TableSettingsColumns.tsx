import { FC, useState, useMemo, useRef, useEffect, useCallback } from "preact/compat";
import "./style.scss";
import Checkbox from "../../Main/Checkbox/Checkbox";
import Button from "../../Main/Button/Button";
import TableSettingsItem from "./TableSettingsItem";
import { useDragColumn } from "../hooks/useDragColumn";
import { arrayMove } from "../../../utils/array";
import useEventListener from "../../../hooks/useEventListener";
import SelectLimit from "../../Main/Pagination/SelectLimit/SelectLimit";
import { sortColumns } from "./utils";
import TableSettingsSearch from "./TableSettingsSearch";
import { TableSettingsProps } from "./TableSettings";
import Tooltip from "../../Main/Tooltip/Tooltip";

export const COLUMN_LIST_SORT_BY = ["alphabetical", "frequency", "unique values"];
export type ColumnListSortBy = typeof COLUMN_LIST_SORT_BY[number];

const TableSettingsColumns: FC<TableSettingsProps> = ({
  columnKeys,
  viewColumnKeys,
  statsByKey,
  dispatchViewColumns,
}) => {
  const itemRefs = useRef<(HTMLDivElement | null)[]>([]);

  const [searchColumn, setSearchColumn] = useState("");
  const [indexFocusItem, setIndexFocusItem] = useState(-1);
  const [sortBy, setSortBy] = useState<ColumnListSortBy>("alphabetical");

  const { foundChecked, foundUnchecked } = useMemo(() => {
    const uncheckedCols = columnKeys.filter(key => !viewColumnKeys.includes(key));
    const filter = (key: string) => key.includes(searchColumn);

    return {
      foundChecked: viewColumnKeys.filter(filter),
      foundUnchecked: uncheckedCols.filter(filter).sort(sortColumns(sortBy, statsByKey))
    };
  }, [columnKeys, searchColumn, viewColumnKeys, sortBy, statsByKey]);

  const commitCheckedOrder = useCallback((nextChecked: string[]) => {
    const set = new Set(nextChecked);

    const positions: number[] = [];
    viewColumnKeys.forEach((c, i) => {
      if (set.has(c)) positions.push(i);
    });

    if (!positions.length) return;

    const next = viewColumnKeys.slice();
    positions.forEach((pos, i) => {
      const v = nextChecked[i];
      if (v != null) next[pos] = v;
    });

    dispatchViewColumns({ type: "replace", columnKeys: next });
  }, [viewColumnKeys]);

  const dragController = useDragColumn({ arr: foundChecked, onChange: commitCheckedOrder, axis: "y" });

  const handleToggleKey = (columnKey: string) => {
    dispatchViewColumns({ type: "toggle", columnKey });
  };

  const handleSelectAll = () => {
    dispatchViewColumns({ type: "replace", columnKeys: foundChecked.concat(foundUnchecked) });
  };

  const handleUnselectAll = () => {
    const nextColumns = viewColumnKeys.filter(key => !foundChecked.includes(key));
    dispatchViewColumns({ type: "replace", columnKeys: nextColumns });
  };

  const handleResetColumns = () => {
    dispatchViewColumns({ type: "reset" });
  };

  const handleResetFocus = () => {
    setIndexFocusItem(-1);
  };

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    const arrowUp = e.key === "ArrowUp";
    const arrowDown = e.key === "ArrowDown";
    const enter = e.key === "Enter";
    const esc = e.key === "Escape";
    const shift = e.shiftKey;

    if (!(arrowUp || arrowDown || enter || esc)) return;

    e.preventDefault();

    // Shift+Up/Down: reorder only checked
    if (shift && (arrowUp || arrowDown)) {
      if (indexFocusItem < 0 || indexFocusItem >= foundChecked.length) return;

      const from = indexFocusItem;
      const to = arrowUp ? from - 1 : from + 1;
      if (to < 0 || to >= foundChecked.length) return;

      const nextChecked = arrayMove(foundChecked, from, to);
      commitCheckedOrder(nextChecked);

      setIndexFocusItem(to);
      return;
    }

    if (arrowDown) {
      setIndexFocusItem(prev => {
        const isLast = prev === foundChecked.length + foundUnchecked.length - 1;
        return isLast ? prev : prev + 1;
      });
    } else if (arrowUp) {
      setIndexFocusItem(prev => (prev - 1 < 0 ? prev : prev - 1));
    } else if (enter) {
      const isUncheckedArr = indexFocusItem >= foundChecked.length;
      const idx = isUncheckedArr ? indexFocusItem - foundChecked.length : indexFocusItem;
      const key = isUncheckedArr ? foundUnchecked[idx] : foundChecked[idx];
      if (key) handleToggleKey(key);
    }
  }, [indexFocusItem, foundChecked, foundUnchecked, commitCheckedOrder, handleToggleKey]);

  useEffect(() => {
    const el = itemRefs.current[indexFocusItem];
    if (!el) return;
    el.scrollIntoView({ block: "nearest" });
  }, [indexFocusItem]);

  useEffect(() => {
    itemRefs.current = [];
    handleResetFocus();
  }, [searchColumn]);

  useEffect(() => {
    return handleResetFocus;
  }, []);

  useEventListener("keydown", handleKeyDown);
  useEventListener("click", handleResetFocus);

  return (
    <>
      <div className="vm-table-settings-section">
        <TableSettingsSearch
          searchColumn={searchColumn}
          onChange={setSearchColumn}
        />
      </div>

      <div className="vm-table-settings-section">
        <div className="vm-table-settings-section-header">
          <Checkbox
            checked={!!foundChecked.length}
            disabled={!foundChecked.length}
            onChange={handleUnselectAll}
          />
          <div className="vm-table-settings-section-header__title">
            Selected fields <span>({foundChecked.length})</span>
          </div>
          <div className="vm-table-settings-section-header__actions">
            <Tooltip title="Resets selected columns and their order">
              <Button
                size="small"
                color="gray"
                variant="outlined"
                onClick={handleResetColumns}
              >
                Reset columns
              </Button>
            </Tooltip>
          </div>
        </div>

        <table className="vm-table-settings-section-list">
          <tbody>
            {foundChecked.map((field, idx) => (
              <TableSettingsItem
                isChecked
                idx={idx}
                key={field}
                field={field}
                dragController={dragController}
                isFocus={idx === indexFocusItem}
                onToggleKey={handleToggleKey}
                rowRefs={itemRefs}
              />
          ))}
          </tbody>
        </table>
      </div>

      <div className="vm-table-settings-section">
        <div className="vm-table-settings-section-header">
          <Checkbox
            checked={false}
            disabled={!foundUnchecked.length}
            onChange={handleSelectAll}
          />
          <div className="vm-table-settings-section-header__title">
            Fields <span>({foundUnchecked.length})</span>
          </div>
          <div className="vm-table-settings-section-header__actions">
            <SelectLimit
              label="Sort by"
              options={COLUMN_LIST_SORT_BY}
              limit={sortBy}
              onChange={setSortBy}
            />
          </div>
        </div>

        <table className="vm-table-settings-section-list">
          <tbody>
            {foundUnchecked.map((field, idx) => (
              <TableSettingsItem
                idx={idx}
                key={field}
                field={field}
                stats={statsByKey.get(field)}
                isFocus={(foundChecked.length + idx) === indexFocusItem}
                onToggleKey={handleToggleKey}
                rowRefs={itemRefs}
              />
          ))}
          </tbody>
        </table>
      </div>
    </>
  );
};

export default TableSettingsColumns;
