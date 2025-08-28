import { FC, useMemo } from "preact/compat";
import { useFieldFilter, useStreamFieldFilter } from "../../hooks/useFieldFilter";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useFetchLogs } from "../../../QueryPage/hooks/useFetchLogs";
import { useExtraFilters } from "../../hooks/useExtraFilters";
import { useEffect } from "react";
import "./style.scss";
import LineLoader from "../../../../components/Main/LineLoader/LineLoader";
import Alert from "../../../../components/Main/Alert/Alert";
import { useOverviewState } from "../../../../state/overview/OverviewStateContext";
import { cardinalityConfig } from "./cardinalityConfig";
import Tooltip from "../../../../components/Main/Tooltip/Tooltip";

type Props = {
  scope: "field" | "stream";
}

const FieldCardinalityCard: FC<Props> = ({ scope }) => {
  const { totalLogs, fieldNames, streamsFieldNames } = useOverviewState();
  const { fieldFilter } = useFieldFilter();
  const { streamFieldFilter } = useStreamFieldFilter();
  const field = scope === "field" ? fieldFilter : streamFieldFilter;
  const fieldNameList = scope === "field" ? fieldNames : streamsFieldNames;

  const { period } = useTimeState();
  const { logs, isLoading, error, fetchLogs, abortController } = useFetchLogs();
  const { extraParams } = useExtraFilters();

  const targetField = fieldNameList.find(f => f.value === field);
  const coverage = targetField?.hits || 0;

  const cardinalityData = useMemo(() => {
    const row = logs?.[0];
    const distinct = Number(row?.distinct ?? 0);
    const ratio = coverage > 0 ? (distinct / coverage) * 100 : null;
    const coverageOfTotal = totalLogs > 0 ? (coverage / totalLogs) * 100 : null;

    return { distinct, coverage, ratio, coverageOfTotal };
  }, [logs, totalLogs, field, coverage]);

  useEffect(() => {
    if (!field) return;

    const query = `${field}:* | stats count_uniq(${field}) as distinct`;
    fetchLogs({ period, extraParams, query });

    return () => abortController.abort();
  }, [period, extraParams.toString(), field, scope]);

  if (!field) return null;

  return (
    <div className="vm-cardinality-card">
      {isLoading && <LineLoader/>}
      {error && <Alert variant="error">{error}</Alert>}

      <div className="vm-top-fields-header">
        <h2 className="vm-title vm-top-fields-header__title">
          {scope === "stream" ? "Stream field" : "Field"} cardinality: <b>`{field}`</b>
        </h2>

        <div className="vm-top-fields-header__configs">

        </div>
      </div>
      <div className="vm-cardinality-card-body">
        {cardinalityConfig.map((config) => (
          <Tooltip
            key={config.key}
            title={<div className="vm-cardinality-card-body-item__description">{config.description({ field })}</div>}
            placement="bottom-right"
          >
            <div className="vm-cardinality-card-body-item">
              <span className="vm-cardinality-card-body-item__title">
                {config.title}
              </span>
              <span className="vm-cardinality-card-body-item__value">
                {config.format(cardinalityData[config.key])}
              </span>
            </div>
          </Tooltip>
        ))}
      </div>
    </div>
  );
};

export default FieldCardinalityCard;
