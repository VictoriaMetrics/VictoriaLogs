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
import { useHitsChartConfig } from "../../../../pages/QueryPage/HitsPanel/hooks/useHitsChartConfig";
import { useExtraFilters } from "../../../ExtraFilters/hooks/useExtraFilters";
import { useFetchFieldNames } from "../../../../pages/OverviewPage/hooks/useFetchFieldNames";
import { getDefaultIntervalOption, getIntervalOptions } from "../../../../utils/intervals";
import { useTimePeriod } from "../../../../pages/QueryPage/hooks/useTimePeriod";
import usePrevious from "../../../../hooks/usePrevious";

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
  const { period: { start, end } } = useTimePeriod();
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

  const { intervals, defaultStep } = useMemo(() => {
    const intervalOptions = getIntervalOptions({ start, end });
    const options = intervalOptions.map(ops => ops.duration);
    const fallbackStep = options[Math.floor(options.length / 2)];

    return {
      intervals: options,
      defaultStep: getDefaultIntervalOption({ start, end })?.duration || fallbackStep,
    };
  }, [start, end]);

  const prevDefaultStep = usePrevious(defaultStep);

  const fieldNamesOptions = useMemo(() => {
    const fields = fieldNames.map(v => v.value).sort((a, b) => a.localeCompare(b));
    return [WITHOUT_GROUPING, ...fields];
  }, [fieldNames]);

  const handleOpenFields = useCallback(() => {
    const period = { start, end };
    void fetchFieldNames({ period, extraParams, skipNoiseFields: true, query });
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
    if (!prevDefaultStep || prevDefaultStep === defaultStep) return;

    const t = setTimeout(() => {
      step.set(defaultStep, { replace: true });
    }, 200);

    return () => clearTimeout(t);
  }, [defaultStep]);

  useEffect(() => {
    if (!defaultStep || !step.value) return;

    const isValidStep = intervals.includes(step.value);
    if (isValidStep) return;

    step.set(defaultStep, { replace: true });
  }, [step.value, step.set, defaultStep, intervals]);

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
