import { FC, useMemo, useRef, useState, MouseEvent } from "preact/compat";
import classNames from "classnames";
import { Series } from "uplot";
import { LegendLogHits } from "../../../../api/types";
import { getStreamPairs } from "../../../../utils/logs";
import { formatNumberShort } from "../../../../utils/math";
import Popper from "../../../Main/Popper/Popper";
import useBoolean from "../../../../hooks/useBoolean";
import LegendHitsMenu from "../LegendHitsMenu/LegendHitsMenu";
import { ExtraFilter } from "../../../../pages/OverviewPage/FiltersBar/types";
import { useCallback } from "react";
import useLegendHitsVisibilityMenu from "./hooks/useLegendHitsVisibilityMenu";

interface Props {
  legend: LegendLogHits;
  series: Series[];
  onRedrawGraph: () => void;
  onApplyFilter: (value: ExtraFilter) => void;
}

const BarHitsLegendItem: FC<Props> = ({ legend, series, onRedrawGraph, onApplyFilter }) => {
  const {
    value: openContextMenu,
    setTrue: handleOpenContextMenu,
    setFalse: handleCloseContextMenu,
  } = useBoolean(false);

  const legendRef = useRef<HTMLDivElement>(null);
  const [clickPosition, setClickPosition] = useState<{ top: number; left: number } | null>(null);

  const targetSeries = useMemo(() => series.find(s => s.label === legend.label), [series]);
  const isOnlyTargetVisible = series.every(s => s === targetSeries || !s.show);

  const fields = useMemo(() => getStreamPairs(legend.label), [legend.label]);

  const label = fields.join(", ");
  const totalShortFormatted = formatNumberShort(legend.total);

  const handleContextMenu = (e: MouseEvent<HTMLDivElement>) => {
    e.preventDefault();
    setClickPosition({ top: e.clientY, left: e.clientX });
    handleOpenContextMenu();
  };

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

  const handleClickByLegend = (e: MouseEvent<HTMLDivElement>) => {
    const { ctrlKey, metaKey, altKey } = e;

    // alt + key // see useLegendHitsVisibilityMenu.tsx
    if (altKey) {
      handleVisibilityToggle();
      return;
    }

    // cmd/ctrl + click // see useLegendHitsVisibilityMenu.tsx
    const ctrlMetaKey = ctrlKey || metaKey;
    if (ctrlMetaKey) {
      handleFocusToggle();
      return;
    }

    handleContextMenu(e);
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

      <Popper
        placement="fixed"
        open={openContextMenu}
        buttonRef={legendRef}
        placementPosition={clickPosition}
        onClose={handleCloseContextMenu}
      >
        <LegendHitsMenu
          legend={legend}
          fields={fields}
          optionsVisibilitySection={optionsVisibilitySection}
          onApplyFilter={onApplyFilter}
          onClose={handleCloseContextMenu}
        />
      </Popper>
    </div>
  );
};

export default BarHitsLegendItem;
