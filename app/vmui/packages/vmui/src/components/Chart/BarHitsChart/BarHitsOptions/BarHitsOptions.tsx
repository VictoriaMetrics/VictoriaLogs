import { FC, useEffect, useMemo } from "preact/compat";
import { GraphOptions, GRAPH_STYLES, GRAPH_QUERY_MODE } from "../types";
import Switch from "../../../Main/Switch/Switch";
import "./style.scss";
import useStateSearchParams from "../../../../hooks/useStateSearchParams";
import { useSearchParams } from "react-router-dom";
import Button from "../../../Main/Button/Button";
import { KeyboardIcon, MoreIcon, VisibilityIcon, VisibilityOffIcon } from "../../../Main/Icons";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import ShortcutKeys from "../../../Main/ShortcutKeys/ShortcutKeys";
import { useCallback } from "react";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import Modal from "../../../Main/Modal/Modal";
import useBoolean from "../../../../hooks/useBoolean";
import SelectLimit from "../../../Main/Pagination/SelectLimit/SelectLimit";
import { WITHOUT_GROUPING } from "../../../../constants/logs";
import { useHitsChartConfig } from "../../../../pages/QueryPage/HitsChart/hooks/useHitsChartConfig";
import { useExtraFilters } from "../../../ExtraFilters/hooks/useExtraFilters";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useFetchFieldNames } from "../../../../pages/OverviewPage/hooks/useFetchFieldNames";
import { humanizeSeconds } from "../../../../utils/time";
import { generateIntervalsMs } from "../../../../utils/intervals";

interface Props {
  query?: string;
  isHitsMode?: boolean;
  isOverview?: boolean;
  onChange: (options: GraphOptions) => void;
}

const BarHitsOptions: FC<Props> = ({ query, isHitsMode, isOverview, onChange }) => {
  const { isMobile } = useDeviceDetect();
  const {
    value: openList,
    toggle: handleToggleList,
    setFalse: handleCloseList,
  } = useBoolean(false);

  const [searchParams, setSearchParams] = useSearchParams();

  const { topHits, groupFieldHits, step } = useHitsChartConfig();

  const { extraParams } = useExtraFilters();
  const { period: { start, end } } = useTimeState();
  const { fetchFieldNames, fieldNames, loading, error } = useFetchFieldNames();

  const [queryMode, setQueryMode] = useStateSearchParams(GRAPH_QUERY_MODE.hits, "graph_mode");
  const isStatsMode = queryMode === GRAPH_QUERY_MODE.stats;

  const hasGroupField = groupFieldHits.value !== WITHOUT_GROUPING;
  const isGroupsLimitVisible = (isHitsMode && hasGroupField) || isStatsMode;

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

  const intervals = useMemo(() => {
    const msIntervals = generateIntervalsMs(start, end);
    return msIntervals.map(ms => humanizeSeconds(ms / 1000));
  }, [start, end]);

  const defaultStep = intervals[Math.floor(intervals.length / 2)];

  const fieldNamesOptions = useMemo(() => {
    const fields = fieldNames.map(v => v.value).sort((a, b) => a.localeCompare(b));
    return [WITHOUT_GROUPING, ...fields];
  }, [fieldNames]);

  const handleOpenFields = useCallback(() => {
    fetchFieldNames({ start, end, extraParams, skipNoiseFields: true, query });
  }, [start, end, extraParams.toString(), fetchFieldNames, query]);

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

  useEffect(() => {
    const isAllowed = (v: string | null) => !!v && intervals.includes(v);
    const shouldReset = (v: string | null) => !isAllowed(v) && v !== defaultStep;

    if (!shouldReset(step.value)) return;

    const t = setTimeout(() => {
      if (shouldReset(step.value)) step.set(defaultStep);
    }, 200);

    return () => clearTimeout(t);
  }, [intervals, defaultStep, step.value]);

  const controls = (
    <>
      <div className="vm-bar-hits-options vm-bar-hits-options_selections">
        <div className="vm-bar-hits-options-item">
          <SelectLimit
            label="Interval"
            options={intervals}
            allowUnlimited={false}
            emptyValueLabel="-"
            limit={step.value || defaultStep}
            onChange={step.set}
          />
        </div>
        {isHitsMode && (
          <>
            <div className="vm-bar-hits-options-item">
              <SelectLimit
                searchable
                label="Group by"
                limit={groupFieldHits.value}
                options={fieldNamesOptions}
                textNoOptions={"No fields found"}
                isLoading={loading}
                error={error ? String(error) : ""}
                onOpenSelect={handleOpenFields}
                onChange={groupFieldHits.set}
              />
            </div>
          </>
        )}
        {isGroupsLimitVisible && (
          <div className="vm-bar-hits-options-item">
            <SelectLimit
              label="Groups limit"
              options={[5, 10, 25, 50]}
              limit={topHits.value}
              onChange={topHits.set}
            />
          </div>
        )}
      </div>

      <div className="vm-bar-hits-options-item vm-bar-hits-options-item_switch">
        <Switch
          label={"Cumulative"}
          value={cumulative}
          onChange={handleChangeCumulative}
        />
      </div>
      {!isOverview && (
        <div className="vm-bar-hits-options-item vm-bar-hits-options-item_switch">
          <Switch
            label="Stats view"
            value={isStatsMode}
            onChange={handleChangeMode}
          />
        </div>
      )}
      <div className="vm-bar-hits-options-item vm-bar-hits-options-item_switch">
        <Switch
          label={"Stacked"}
          value={stacked}
          onChange={handleChangeStacked}
        />
      </div>
    </>
  );

  return (
    <div
      className={classNames({
        "vm-bar-hits-options": true,
        "vm-bar-hits-options_mobile": isMobile,
      "vm-bar-hits-options_hidden": hideChart,
      })}
    >
      {!isMobile && !hideChart && (
        <>
          {controls}
          <ShortcutKeys withHotkey={false}>
            <Button
              variant="text"
              color="gray"
              startIcon={<KeyboardIcon/>}
            />
          </ShortcutKeys>
        </>
      )}
      {hideChart && (
        <div className="vm-bar-hits-options__hidden-info">
          Hits chart is hidden. Data updates are paused.
        </div>
      )}

      <Tooltip title={hideChart ? "Show chart and resume hits updates" : "Hide chart and pause hits updates"}>
        <Button
          variant="text"
          color="primary"
          startIcon={hideChart ? <VisibilityIcon/> : <VisibilityOffIcon/>}
          onClick={toggleHideChart}
          aria-label="settings"
        >
          {hideChart ? "Show chart" : ""}
        </Button>
      </Tooltip>

      {isMobile && (
        <>
          <Button
            variant="text"
            color="primary"
            startIcon={<MoreIcon/>}
            onClick={handleToggleList}
            aria-label="settings"
          />
          <Modal
            title={"Hits Options"}
            onClose={handleCloseList}
            isOpen={openList}
            className={classNames({
              "vm-header-controls-modal": true,
              "vm-header-controls-modal_open": openList,
            })}
          >
            <div className="vm-bar-hits-options vm-bar-hits-options_mobile">
              {controls}
            </div>
          </Modal>
        </>
      )}
    </div>
  );
};

export default BarHitsOptions;
