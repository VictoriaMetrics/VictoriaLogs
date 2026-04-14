import { FC, useEffect, useRef, useState } from "preact/compat";
import { CloseIcon } from "../../Main/Icons";
import { ExtraFilter } from "../types";
import "./style.scss";
import { useCallback } from "react";
import Tooltip from "../../Main/Tooltip/Tooltip";

type Props = {
  filter: ExtraFilter & { label: string };
  onRemove: (field: string, value: string) => void;
}

const ExtraFiltersPanelItem: FC<Props> = ({ filter, onRemove }) => {
  const labelRef = useRef<HTMLDivElement>(null);
  const [isOverflownLabel, setIsOverflownLabel] = useState(false);

  const handleRemove = useCallback(() => {
    onRemove(filter.field, filter.value);
  }, [filter, onRemove]);

  useEffect(() => {
    if (!labelRef.current) return;
    setIsOverflownLabel(labelRef.current.scrollWidth > labelRef.current.clientWidth);
  }, [filter.label, labelRef]);

  return (
    <Tooltip
      title={<p className="vm-extra-filters-panel-item__tooltip">{filter.value}</p>}
      disabled={!isOverflownLabel}
    >
      <div
        key={filter.label}
        className="vm-extra-filters-panel-item"
      >

        <div
          className="vm-extra-filters-panel-item__label"
          ref={labelRef}
        >
          {filter.label}
        </div>
        <div
          className="vm-extra-filters-panel-item__remove"
          onClick={handleRemove}
        >
          <CloseIcon/>
        </div>
      </div>
    </Tooltip>
  );
};

export default ExtraFiltersPanelItem;
