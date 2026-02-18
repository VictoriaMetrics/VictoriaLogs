import { FC } from "preact/compat";
import { DragIcon } from "../../Main/Icons";
import "./style.scss";
import Checkbox from "../../Main/Checkbox/Checkbox";
import classNames from "classnames";
import Button from "../../Main/Button/Button";
import { transparentImg } from "./utils";
import { formatNumber, formatPercent } from "../../../utils/number";
import { UseDragColumnReturn } from "../hooks/useDragColumn";
import { LogsColumnStats } from "../../Views/TableView/hooks/useTableLogsKeys";
import { MutableRef } from "preact/hooks";
import Tooltip from "../../Main/Tooltip/Tooltip";

interface Props {
  idx: number;
  field: string;
  stats?: LogsColumnStats;
  isFocus: boolean;
  isChecked?: boolean;
  rowRefs: MutableRef<(HTMLDivElement | null)[]>;
  onToggleKey: (key: string) => void;
  dragController?: UseDragColumnReturn;
}

const TableSettingsItem: FC<Props> = ({
  idx,
  field,
  stats,
  isFocus,
  isChecked = false,
  rowRefs,
  onToggleKey,
  dragController,
}) => {
  const shiftUnits = dragController?.getShiftUnits?.(idx) || 0;
  const shiftPx = shiftUnits * (dragController?.itemSize || 0);

  const handleChange = () => {
    onToggleKey(field);
  };

  const handleClickByGrab = (e: MouseEvent) => {
    e.stopPropagation();
    e.preventDefault();
  };

  const handleDragStart = (e: DragEvent) => {
    const dt = e.dataTransfer;
    if (!dt) return;

    const rowEl = rowRefs.current?.[idx];
    dragController?.onDragKeyStart?.(field, rowEl);

    dt.effectAllowed = "move";
    dt.setData("text/plain", field);
    dt.setDragImage(transparentImg, 0, 0);
  };

  const handleDragOver = (e: DragEvent) => {
    e.preventDefault();

    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const y = e.clientY - rect.top;
    const pos = y < rect.height / 2 ? "before" : "after";

    dragController?.onDragOverKeyLive?.(field, pos);
  };

  const handleSetRef = (e: HTMLDivElement | null) => {
    rowRefs.current[idx] = e;
  };

  return (
    <tr
      className={classNames({
        "vm-table-settings-section-list-row": true,
        "vm-table-settings-section-list-row_focus": isFocus,
        "vm-table-settings-section-list-row_dragging": dragController?.draggingKey === field,
        "vm-table-settings-section-list-row_dropping": dragController?.isDropping,
      })}
      style={{ transform: `translateY(${shiftPx}px)` }}
      ref={handleSetRef}
      onDragOver={handleDragOver}
      onClick={handleChange}
    >
      <td className="vm-table-settings-section-list-cell vm-table-settings-section-list-cell_checkbox">
        <Checkbox checked={isChecked}/>
      </td>

      <td className="vm-table-settings-section-list-cell vm-table-settings-section-list-cell_title">
        {field}
      </td>

      {!isChecked && (
        <>
          {stats?.uniqValuesCount && (
            <Tooltip title="Unique values in this field">
              <td className="vm-table-settings-section-list-cell vm-table-settings-section-list-cell_stats">
                {formatNumber(stats.uniqValuesCount)}
              </td>
            </Tooltip>

          )}
          {stats?.frequencyPercent && (
            <Tooltip title="Field present in rows (%)">
              <td className="vm-table-settings-section-list-cell vm-table-settings-section-list-cell_stats">
                {formatPercent(stats.frequencyPercent, 0)}
              </td>
            </Tooltip>
          )}
        </>
      )}

      {isChecked && (
        <td
          className="vm-table-settings-section-list-cell vm-table-settings-section-list-cell_drag-handle"
          onDragStart={handleDragStart}
          onDragEnd={dragController?.onDragEnd}
          draggable={isChecked}
        >
          <Button
            variant="text"
            color="gray"
            startIcon={<DragIcon/>}
            className="vm-grab-handle"
            onClick={handleClickByGrab}
          />
        </td>
      )}
    </tr>
  );
};

export default TableSettingsItem;
