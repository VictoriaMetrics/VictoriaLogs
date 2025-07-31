import { FC, useCallback } from "preact/compat";
import LegendHitsMenuRow from "./LegendHitsMenuRow";
import { FocusIcon, UnfocusIcon, VisibilityIcon, VisibilityOffIcon } from "../../../Main/Icons";
import { LegendLogHits, LegendLogHitsMenu } from "../../../../api/types";
import { Series } from "uplot";
import { useMemo } from "react";

interface Props {
  legend: LegendLogHits;
  series: Series[];
  onRedrawGraph: () => void;
  onClose: () => void;
}

const LegendHitsMenuVisibility: FC<Props> = ({ legend, series, onRedrawGraph, onClose }) => {

  const targetSeries = useMemo(() => series.find(s => s.label === legend.label), [series]);

  const isShow = Boolean(targetSeries?.show);
  const isOnlyTargetVisible = series.every(s => s === targetSeries || !s.show);

  const handleVisibilityToggle = useCallback(() => {
    if (!targetSeries) return;
    targetSeries.show = !targetSeries.show;
    onRedrawGraph();
    onClose();
  }, [targetSeries, onRedrawGraph, onClose]);

  const handleFocusToggle = useCallback(() => {
    series.forEach(s => {
      s.show = isOnlyTargetVisible || (s === targetSeries);
    });
    onRedrawGraph();
    onClose();
  }, [series, isOnlyTargetVisible, targetSeries, onRedrawGraph, onClose]);

  const options: LegendLogHitsMenu[] = useMemo(() => [
    {
      title: isShow ? "Hide series" : "Show series",
      icon: isShow ? <VisibilityOffIcon/> : <VisibilityIcon/>,
      handler: handleVisibilityToggle,
    },
    {
      title: isOnlyTargetVisible ? "Show all series" : "Focus on series",
      icon: isOnlyTargetVisible ? <UnfocusIcon/> : <FocusIcon/>,
      handler: handleFocusToggle,
    },
  ], [isOnlyTargetVisible, isShow, handleFocusToggle, handleVisibilityToggle]);

  return (
    <div className="vm-legend-hits-menu-section">
      {options.map(({ icon, title, handler }) => (
        <LegendHitsMenuRow
          key={title}
          iconStart={icon}
          title={title}
          handler={handler}
        />
      ))}
    </div>
  );
};

export default LegendHitsMenuVisibility;
