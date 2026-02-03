import { FC, useEffect, useMemo } from "preact/compat";
import { GraphOptions, GRAPH_STYLES, GRAPH_QUERY_MODE } from "../types";
import Switch from "../../../Main/Switch/Switch";
import "./style.scss";
import useStateSearchParams from "../../../../hooks/useStateSearchParams";
import { useSearchParams } from "react-router-dom";
import Button from "../../../Main/Button/Button";
import { TipIcon, VisibilityIcon, VisibilityOffIcon } from "../../../Main/Icons";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import ShortcutKeys from "../../../Main/ShortcutKeys/ShortcutKeys";
import { useCallback } from "react";

interface Props {
  isOverview?: boolean;
  onChange: (options: GraphOptions) => void;
}

const BarHitsOptions: FC<Props> = ({ isOverview, onChange }) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [queryMode, setQueryMode] = useStateSearchParams(GRAPH_QUERY_MODE.hits, "graph_mode");
  const isStatsMode = queryMode === GRAPH_QUERY_MODE.stats;

  const [stacked, setStacked] = useStateSearchParams(false, "stacked");
  const [cumulative, setCumulative] = useStateSearchParams(false, "cumulative");
  const [hideChart, setHideChart] = useStateSearchParams(false, "hide_chart");

  const options: GraphOptions = useMemo(() => ({
    graphStyle: GRAPH_STYLES.BAR,
    queryMode,
    stacked,
    cumulative,
    fill: true,
    hideChart,
  }), [stacked, cumulative, hideChart, queryMode]);

  const handleChangeSearchParams = useCallback((key: string, shouldSet: boolean, paramValue?: string) => {
    const next = new URLSearchParams(searchParams);
    shouldSet ? next.set(key, paramValue ?? String(shouldSet)) : next.delete(key);
    setSearchParams(next);
  }, [searchParams, setSearchParams]);

  const handleChangeMode = useCallback((val: boolean) => {
    const mode = val ? GRAPH_QUERY_MODE.stats : GRAPH_QUERY_MODE.hits;
    setQueryMode(mode);
    handleChangeSearchParams("graph_mode", val, mode);
  }, [setQueryMode, handleChangeSearchParams]);

  const handleChangeStacked = useCallback((val: boolean) => {
    setStacked(val);
    handleChangeSearchParams("stacked", val);
  }, [setStacked, handleChangeSearchParams]);

  const handleChangeCumulative = useCallback((val: boolean) => {
    setCumulative(val);
    handleChangeSearchParams("cumulative", val);
  }, [setCumulative, handleChangeSearchParams]);

  const toggleHideChart = useCallback(() => {
    setHideChart(prev => {
      const nextVal = !prev;
      handleChangeSearchParams("hide_chart", nextVal);
      return nextVal;
    });
  }, [setHideChart, handleChangeSearchParams]);

  useEffect(() => {
    onChange(options);
  }, [options]);

  return (
    <div className="vm-bar-hits-options">
      <div className="vm-bar-hits-options-item">
        <Switch
          label={"Cumulative"}
          value={cumulative}
          onChange={handleChangeCumulative}
        />
      </div>
      {!isOverview && (
        <div className="vm-bar-hits-options-item">
          <Switch
            label="Stats view"
            value={isStatsMode}
            onChange={handleChangeMode}
          />
        </div>
      )}
      <div className="vm-bar-hits-options-item">
        <Switch
          label={"Stacked"}
          value={stacked}
          onChange={handleChangeStacked}
        />
      </div>
      <ShortcutKeys>
        <Button
          variant="text"
          color="gray"
          startIcon={<TipIcon/>}
        />
      </ShortcutKeys>
      <Tooltip title={hideChart ? "Show chart and resume hits updates" : "Hide chart and pause hits updates"}>
        <Button
          variant="text"
          color="primary"
          startIcon={hideChart ? <VisibilityOffIcon/> : <VisibilityIcon/>}
          onClick={toggleHideChart}
          ariaLabel="settings"
        />
      </Tooltip>
    </div>
  );
};

export default BarHitsOptions;
