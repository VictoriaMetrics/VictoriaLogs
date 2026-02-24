import { FC, useState, useRef, useCallback } from "preact/compat";
import { CloseIcon } from "../../Main/Icons";
import "./style.scss";
import Button from "../../Main/Button/Button";
import useEventListener from "../../../hooks/useEventListener";
import { useLocation, useNavigate } from "react-router-dom";
import DragResizeHandle from "../../Main/DragResizeHandle/DragResizeHandle";
import { Size, useResizeObserver } from "../../../hooks/useResizeObserver";
import { TableSettingsProps } from "./TableSettings";
import TableSettingsColumns from "./TableSettingsColumns";
import { getFromStorage, saveToStorage } from "../../../utils/storage";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import classNames from "classnames";

interface Props extends TableSettingsProps {
  title: string;
  onClose: () => void
}

const WIDTH_STORAGE_KEY = "LOGS_TABLE_DRAWER";

const getDefaultWidth = () => {
  const widthFromStorage = getFromStorage(WIDTH_STORAGE_KEY);
  return widthFromStorage ? Number(widthFromStorage) : 0;
};

const TableSettingsDrawer: FC<Props> = ({
  title,
  columnKeys,
  viewColumnKeys,
  statsByKey,
  dispatchViewColumns,
  onClose
}) => {
  const navigate = useNavigate();
  const location = useLocation();
  const { isMobile } = useDeviceDetect();

  const [containerWidth, setContainerWidth] = useState(getDefaultWidth());

  const containerRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState<Size>({
    width: containerRef?.current?.offsetWidth,
    height: containerRef?.current?.offsetHeight
  });
  useResizeObserver({ ref: containerRef, onResize: setSize });

  const handleKeyDown = (e: KeyboardEvent) => {
    const esc = e.key === "Escape";

    if (esc) {
      onClose();
      return;
    }
  };

  const handleResizeWidth = (width: number) => {
    setContainerWidth(width);
    saveToStorage(WIDTH_STORAGE_KEY, String(width));
  };

  const handlePopstate = useCallback(() => {
    navigate(location, { replace: true });
    onClose();
  }, [navigate, location]);

  useEventListener("popstate", handlePopstate);
  useEventListener("keydown", handleKeyDown);

  return (
    <div className="vm-table-settings-wrapper">
      <div
        className={classNames("vm-table-settings", { "vm-table-settings_mobile": isMobile })}
        style={containerWidth && !isMobile ? { width: `${containerWidth}px` } : undefined}
        ref={containerRef}
      >
        <div className="vm-table-settings-header">
          <div className="vm-table-settings-header__title">{title}</div>

          <Button
            variant="text"
            onClick={onClose}
            startIcon={<CloseIcon/>}
          />
        </div>

        <TableSettingsColumns
          columnKeys={columnKeys}
          viewColumnKeys={viewColumnKeys}
          statsByKey={statsByKey}
          dispatchViewColumns={dispatchViewColumns}
        />
      </div>

      <DragResizeHandle
        targetRef={containerRef}
        minSize={250}
        dir={-1}
        size={size}
        onResizeEnd={handleResizeWidth}
      />
    </div>
  );
};

export default TableSettingsDrawer;
