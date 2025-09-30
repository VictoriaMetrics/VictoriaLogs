import { FC, useEffect, useMemo } from "preact/compat";
import { useTimeState } from "../../../state/time/TimeStateContext";
import { useFetchLogs } from "../../QueryPage/hooks/useFetchLogs";
import { filterToExpr, useExtraFilters } from "../hooks/useExtraFilters";
import { useFieldFilter, useStreamFieldFilter } from "../hooks/useFieldFilter";
import QueryPageBody from "../../QueryPage/QueryPageBody/QueryPageBody";
import Alert from "../../../components/Main/Alert/Alert";
import { ExtraFilterOperator } from "../FiltersBar/types";
import { useState } from "react";
import SelectLimit from "../../../components/Main/Pagination/SelectLimit/SelectLimit";
import "./style.scss";
import { Link, useSearchParams } from "react-router-dom";
import Button from "../../../components/Main/Button/Button";
import { CopyIcon, DoneIcon, OpenNewIcon } from "../../../components/Main/Icons";
import useCopyToClipboard from "../../../hooks/useCopyToClipboard";
import router from "../../../router";
import { escapeDoubleQuotes } from "../../../utils/regexp";

const operator = ExtraFilterOperator.Equals;

const getQueryFromArray = (field: string, values: string[]) => {
  const escapeValues = values.map(v => `"${escapeDoubleQuotes(v)}"`);
  return `${field}:in(\n${escapeValues.join(",\n")}\n)`;
};

const OverviewLogs:FC = () => {
  const [searchParams] = useSearchParams();

  const { period, relativeTime, duration } = useTimeState();
  const { logs, isLoading, error, fetchLogs, abortController } = useFetchLogs();
  const { extraParams } = useExtraFilters();
  const { fieldFilter, fieldValueFilters } = useFieldFilter();
  const { streamFieldFilter, streamFieldValueFilters } = useStreamFieldFilter();
  const copyToClipboard = useCopyToClipboard();

  const [copied, setCopied] = useState<boolean>(false);
  const [limit, setLimit] = useState(10);
  const hidePreviewLogs = useMemo(() => Boolean(searchParams.get("hide_logs")), [searchParams]);

  const query = useMemo(() => {
    const queryParts: string[] = [];

    if (streamFieldFilter && streamFieldValueFilters.length) {
      const filterByStream = getQueryFromArray(streamFieldFilter, streamFieldValueFilters);
      queryParts.push(filterByStream);
    } else if (streamFieldFilter) {
      const filterByStream = filterToExpr({ field: streamFieldFilter, value: "*", operator });
      queryParts.push(filterByStream);
    }

    if (fieldFilter && fieldValueFilters.length) {
      const filterByField = getQueryFromArray(fieldFilter, fieldValueFilters);
      queryParts.push(filterByField);
    } else if (fieldFilter) {
      const filterByField = filterToExpr({ field: fieldFilter, value: "*", operator });
      queryParts.push(filterByField);
    }

    const extraFieldsFilters = extraParams.getAll("extra_filters");
    const extraStreamFilters = extraParams.getAll("extra_stream_filters");
    const extraFilters = extraFieldsFilters.concat(extraStreamFilters);
    if (extraFilters.length) {
      queryParts.push(...extraFilters);
    }

    return queryParts.length ? queryParts.join("\n") : "*";
  }, [period, fieldFilter, fieldValueFilters, streamFieldFilter, streamFieldValueFilters, extraParams]);

  const linkToLogs = useMemo(() => {
    const params = new URLSearchParams({
      query,
      "g0.range_input": duration,
      "g0.end_input": period.date,
      "g0.relative_time": relativeTime || "none",
    });

    return `${router.home}?${params.toString()}`;
  }, [query, duration, relativeTime]);

  const handleCopyQuery  = async () => {
    await copyToClipboard(query, "Query has been copied");
    setCopied(true);
  };

  useEffect(() => {
    abortController.abort();
    if (hidePreviewLogs) return;
    fetchLogs({ query, period, limit });
  }, [query, limit, hidePreviewLogs]);

  useEffect(() => {
    if (copied === null) return;
    const timeout = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(timeout);
  }, [copied]);

  return (
    <div className="vm-overview-logs vm-block">
      <div className="vm-overview-logs-header">
        <span className="vm-title">Query:</span>
        <div className="vm-overview-logs-query">
          <p className="vm-overview-logs-query__expr">{query}</p>

        </div>
        <div className="vm-overview-logs-header__actions">
          <SelectLimit
            label="Limit"
            limit={limit}
            onChange={setLimit}
          />
          <Button
            size="small"
            variant="text"
            startIcon={copied ? <DoneIcon/> : <CopyIcon/>}
            onClick={handleCopyQuery}
          >
            {copied ? "Copied" : "Copy query"}
          </Button>
          <Link
            to={linkToLogs}
            target="_blank"
            rel="noreferrer"
          >
            <Button
              size="small"
              variant="text"
              startIcon={<OpenNewIcon/>}
            >
              Open query
            </Button>
          </Link>
        </div>
      </div>
      <div>
        {error && <Alert variant="error">{error}</Alert>}
        {!error && (
          <QueryPageBody
            isPreview
            data={logs}
            isLoading={isLoading}
          />
        )}
      </div>
    </div>
  );
};

export default OverviewLogs;
