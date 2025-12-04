import { LegendLogHitsMenu } from "../../../../../api/types";
import { useMemo } from "react";
import { FocusIcon, UnfocusIcon, VisibilityIcon, VisibilityOffIcon } from "../../../../Main/Icons";
import { altKeyLabel, ctrlKeyLabel } from "../../../../../utils/keyboard";
import { Series } from "uplot";

type Props = {
  targetSeries?: Series;
  isOnlyTargetVisible: boolean;
  handleVisibilityToggle: () => void;
  handleFocusToggle: () => void;
}

const useLegendHitsVisibilityMenu = ({
  targetSeries,
  isOnlyTargetVisible,
  handleVisibilityToggle,
  handleFocusToggle
}: Props): LegendLogHitsMenu[] => {
  const isShow = Boolean(targetSeries?.show);

  return useMemo(() => [
    {
      title: isShow ? "Hide this series" : "Show this series",
      iconStart: isShow ? <VisibilityOffIcon/> : <VisibilityIcon/>,
      shortcut: `${altKeyLabel} + Click`, // handled in BarHitsLegendItem.tsx
      handler: handleVisibilityToggle,
    },
    {
      title: isOnlyTargetVisible ? "Show all series" : "Show only this series",
      iconStart: isOnlyTargetVisible ? <UnfocusIcon/> : <FocusIcon/>,
      shortcut: `${ctrlKeyLabel} + Click`, // handled in BarHitsLegendItem.tsx
      handler: handleFocusToggle,
    },
  ], [isOnlyTargetVisible, isShow, handleVisibilityToggle, handleFocusToggle]);
};

export default useLegendHitsVisibilityMenu;
