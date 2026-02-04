import { FC, useEffect, useMemo } from "preact/compat";
import { GraphOptions, GRAPH_STYLES, GRAPH_QUERY_MODE } from "../types";
import Switch from "../../../Main/Switch/Switch";
import "./style.scss";
import useStateSearchParams from "../../../../hooks/useStateSearchParams";
import { useSearchParams } from "react-router-dom";
import Button from "../../../Main/Button/Button";
import { MoreIcon, TipIcon, VisibilityIcon, VisibilityOffIcon } from "../../../Main/Icons";
import Tooltip from "../../../Main/Tooltip/Tooltip";
import ShortcutKeys from "../../../Main/ShortcutKeys/ShortcutKeys";
import { useCallback } from "react";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import classNames from "classnames";
import Modal from "../../../Main/Modal/Modal";
import useBoolean from "../../../../hooks/useBoolean";
import SelectLimit from "../../../Main/Pagination/SelectLimit/SelectLimit";
import { LOGS_BAR_COUNTS } from "../../../../constants/logs";
import { useHitsChartConfig } from "../../../../pages/QueryPage/HitsChart/hooks/useHitsChartConfig";
import { useExtraFilters } from "../../../../pages/OverviewPage/hooks/useExtraFilters";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useFetchFieldNames } from "../../../../pages/OverviewPage/hooks/useFetchFieldNames";

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

  const { topHits, groupFieldHits, barsCount } = useHitsChartConfig();

  const { extraParams } = useExtraFilters();
  const { period: { start, end } } = useTimeState();
  const { fetchFieldNames, fieldNames, loading, error } = useFetchFieldNames();

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

  const fieldNamesOptions = useMemo(() => {
    return fieldNames.map(v => v.value).sort((a, b) => a.localeCompare(b));
  }, [fieldNames]);

  const handleOpenFields = useCallback(() => {
    fetchFieldNames({ start, end, extraParams, showAllFields: true, query });
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

  const Controls = () => (
    <>
      <div className="vm-bar-hits-options vm-bar-hits-options_selections">
        <div className="vm-bar-hits-options-item">
          <SelectLimit
            label="Top hits"
            options={[5, 10, 25, 50]}
            limit={topHits.value}
            onChange={topHits.set}
          />
        </div>
        <div className="vm-bar-hits-options-item">
          <SelectLimit
            label="Bars"
            options={LOGS_BAR_COUNTS}
            limit={barsCount.value}
            onChange={barsCount.set}
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
      </div>

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
    </>
  );

  return (
    <div
      className={classNames({
      "vm-bar-hits-options": true,
      "vm-bar-hits-options_mobile": isMobile,
    })}
    >
      {!isMobile && (
        <>
          <Controls/>
          <ShortcutKeys>
            <Button
              variant="text"
              color="gray"
              startIcon={<TipIcon/>}
            />
          </ShortcutKeys>
        </>
      )}
      <Tooltip title={hideChart ? "Show chart and resume hits updates" : "Hide chart and pause hits updates"}>
        <Button
          variant="text"
          color="primary"
          startIcon={hideChart ? <VisibilityOffIcon/> : <VisibilityIcon/>}
          onClick={toggleHideChart}
          ariaLabel="settings"
        />
      </Tooltip>

      {isMobile && (
        <>
          <Button
            variant="text"
            color="primary"
            startIcon={<MoreIcon/>}
            onClick={handleToggleList}
            ariaLabel="settings"
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
              <Controls/>
            </div>
          </Modal>
        </>
      )}
    </div>
  );
};

export default BarHitsOptions;
