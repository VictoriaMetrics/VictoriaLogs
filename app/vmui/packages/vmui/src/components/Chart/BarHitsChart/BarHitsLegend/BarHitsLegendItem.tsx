import { FC, useMemo, useRef } from "preact/compat";
import classNames from "classnames";
import { Series } from "uplot";
import { LegendLogHits } from "../../../../api/types";
import { getStreamPairs } from "../../../../utils/logs";
import { formatNumberShort } from "../../../../utils/number";
import Popper from "../../../Main/Popper/Popper";
import useBoolean from "../../../../hooks/useBoolean";
import LegendHitsMenu from "../LegendHitsMenu/LegendHitsMenu";
import { useCallback } from "react";
import useLegendHitsVisibilityMenu from "./hooks/useLegendHitsVisibilityMenu";
import Button from "../../../Main/Button/Button";
import { MoreIcon } from "../../../Main/Icons";

interface Props {
  legend: LegendLogHits;
  series: Series[];
  onRedrawGraph: () => void;
}

const BarHitsLegendItem: FC<Props> = ({ legend, series, onRedrawGraph }) => {
  const {
    value: openContextMenu,
    toggle: toggleContextMenu,
    setFalse: handleCloseContextMenu,
  } = useBoolean(false);

  const legendRef = useRef<HTMLDivElement>(null);

  const targetSeries = useMemo(() => series.find(s => s.label === legend.label), [series]);
  const isOnlyTargetVisible = series.every(s => s === targetSeries || !s.show);

  const fields = useMemo(() => getStreamPairs(legend.label), [legend.label]);

  const label = fields.join(", ");
  const totalShortFormatted = formatNumberShort(legend.total);

  const handleVisibilityToggle = useCallback(() => {
    if (!targetSeries) return;
    targetSeries.show = !targetSeries.show;
    onRedrawGraph();
    handleCloseContextMenu();
  }, [targetSeries, onRedrawGraph, handleCloseContextMenu]);

  const handleFocusToggle = useCallback(() => {
    series.forEach(s => {
      s.show = isOnlyTargetVisible || (s === targetSeries);
    });
    onRedrawGraph();
    handleCloseContextMenu();
  }, [series, isOnlyTargetVisible, targetSeries, onRedrawGraph, handleCloseContextMenu]);

  const handleClickMenu = (e: MouseEvent) => {
    e.stopPropagation();
    toggleContextMenu();
  };

  const handleClickByLegend = (e: MouseEvent) => {
    const { ctrlKey, metaKey } = e;
    const ctrlMetaKey = ctrlKey || metaKey;

    if (ctrlMetaKey) {
      // cmd/ctrl + click // see useLegendHitsVisibilityMenu.tsx
      handleVisibilityToggle();
    } else {
      // click // see useLegendHitsVisibilityMenu.tsx
      handleFocusToggle();
    }
  };

  const optionsVisibilitySection = useLegendHitsVisibilityMenu({
    targetSeries,
    isOnlyTargetVisible,
    handleVisibilityToggle,
    handleFocusToggle
  });

  return (
    <div
      ref={legendRef}
      className={classNames({
        "vm-bar-hits-legend-item": true,
        "vm-bar-hits-legend-item_other": legend.isOther,
        "vm-bar-hits-legend-item_active": openContextMenu,
        "vm-bar-hits-legend-item_hide": !targetSeries?.show,
      })}
      onClick={handleClickByLegend}
    >
      <div
        className="vm-bar-hits-legend-item__marker"
        style={{ backgroundColor: `${legend.stroke}` }}
      />
      <div className="vm-bar-hits-legend-item__label">{label}</div>
      <span className="vm-bar-hits-legend-item__total">({totalShortFormatted})</span>

      <div className="vm-bar-hits-legend-item__actions">
        <Button
          size="small"
          variant="text"
          color="gray"
          startIcon={<MoreIcon/>}
          onClick={handleClickMenu}
        />
      </div>

      <Popper
        placement="bottom-right"
        open={openContextMenu}
        buttonRef={legendRef}
        onClose={handleCloseContextMenu}
      >
        <LegendHitsMenu
          legend={legend}
          fields={fields}
          optionsVisibilitySection={optionsVisibilitySection}
          onClose={handleCloseContextMenu}
        />
      </Popper>
    </div>
  );
};

export default BarHitsLegendItem;
