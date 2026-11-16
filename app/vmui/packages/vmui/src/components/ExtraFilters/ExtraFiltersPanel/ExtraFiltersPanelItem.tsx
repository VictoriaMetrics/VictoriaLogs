import { FC, useEffect, useRef, useState } from "preact/compat";
import { CloseIcon, FieldIcon, FieldStreamIcon } from "../../Main/Icons";
import { ExtraFilter } from "../types";
import "./style.scss";
import { useCallback } from "react";
import Tooltip from "../../Main/Tooltip/Tooltip";
import classNames from "classnames";
import { isStreamFilter } from "../utils/isStreamFilter";

type Props = {
  filter: ExtraFilter & { label: string };
  onRemove: (field: string, value: string) => void;
}

const ExtraFiltersPanelItem: FC<Props> = ({ filter, onRemove }) => {
  const labelRef = useRef<HTMLDivElement>(null);
  const [isOverflownLabel, setIsOverflownLabel] = useState(false);

  const isStream = isStreamFilter(filter);

  const handleRemove = useCallback(() => {
    onRemove(filter.field, filter.value);
  }, [filter, onRemove]);

  useEffect(() => {
    if (!labelRef.current) return;
    setIsOverflownLabel(labelRef.current.scrollWidth > labelRef.current.clientWidth);
  }, [filter.label, labelRef]);

  return (
    <div
      key={filter.label}
      className={classNames({
          "vm-extra-filters-panel-item": true,
          "vm-extra-filters-panel-item_stream": isStream
        })}
    >
      <Tooltip title={`${isStream ? "Stream" : "Field"} filter`}>
        <div className="vm-extra-filters-panel-item__icon">
          {isStream ? <FieldStreamIcon/> : <FieldIcon/>}
        </div>
      </Tooltip>
      <Tooltip
        title={<p className="vm-extra-filters-panel-item__tooltip">{filter.value}</p>}
        disabled={!isOverflownLabel}
      >
        <div
          className="vm-extra-filters-panel-item__label"
          ref={labelRef}
        >
          {filter.label}
        </div>
      </Tooltip>
      <div
        className="vm-extra-filters-panel-item__remove"
        onClick={handleRemove}
      >
        <CloseIcon/>
      </div>
    </div>
  );
};

export default ExtraFiltersPanelItem;
