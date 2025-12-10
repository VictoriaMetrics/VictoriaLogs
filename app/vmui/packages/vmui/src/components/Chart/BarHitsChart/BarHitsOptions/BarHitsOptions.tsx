import { FC, useEffect, useMemo } from "preact/compat";
import { GraphOptions, GRAPH_STYLES } from "../types";
import Switch from "../../../Main/Switch/Switch";
import "./style.scss";
import useStateSearchParams from "../../../../hooks/useStateSearchParams";
import { useSearchParams } from "react-router-dom";
import Button from "../../../Main/Button/Button";
import { TipIcon, VisibilityIcon, VisibilityOffIcon } from "../../../Main/Icons";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import ShortcutKeys from "../../../Main/ShortcutKeys/ShortcutKeys";

interface Props {
  onChange: (options: GraphOptions) => void;
}

const BarHitsOptions: FC<Props> = ({ onChange }) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [stacked, setStacked] = useStateSearchParams(false, "stacked");
  const [hideChart, setHideChart] = useStateSearchParams(false, "hide_chart");

  const options: GraphOptions = useMemo(() => ({
    graphStyle: GRAPH_STYLES.BAR,
    stacked,
    fill: true,
    hideChart,
  }), [stacked, hideChart]);

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
