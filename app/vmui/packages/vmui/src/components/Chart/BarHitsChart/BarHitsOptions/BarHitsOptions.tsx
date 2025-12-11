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

interface Props {
  isOverview?: boolean;
  onChange: (options: GraphOptions) => void;
}

const BarHitsOptions: FC<Props> = ({ isOverview, onChange }) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [queryMode, setQueryMode] = useStateSearchParams(GRAPH_QUERY_MODE.hits, "graph_mode");
  const isStatsMode = queryMode === GRAPH_QUERY_MODE.stats;
  const [stacked, setStacked] = useStateSearchParams(false, "stacked");
  const [hideChart, setHideChart] = useStateSearchParams(false, "hide_chart");

  const options: GraphOptions = useMemo(() => ({
    graphStyle: GRAPH_STYLES.BAR,
    queryMode,
    stacked,
    fill: true,
    hideChart,
  }), [stacked, hideChart, queryMode]);

  const handleChangeMode = (val: boolean) => {
    const mode = val ? GRAPH_QUERY_MODE.stats : GRAPH_QUERY_MODE.hits;
    setQueryMode(mode);
    val ? searchParams.set("graph_mode", mode) : searchParams.delete("graph_mode");
    setSearchParams(searchParams);
  };

  const handleChangeStacked = (val: boolean) => {
    setStacked(val);
    val ? searchParams.set("stacked", "true") : searchParams.delete("stacked");
    setSearchParams(searchParams);
  };

  const toggleHideChart = () => {
    setHideChart(prev => {
      const newVal = !prev;
      newVal ? searchParams.set("hide_chart", "true") : searchParams.delete("hide_chart");
      setSearchParams(searchParams);
      return newVal;
    });
  };

  useEffect(() => {
    onChange(options);
  }, [options]);

  return (
    <div className="vm-bar-hits-options">
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
