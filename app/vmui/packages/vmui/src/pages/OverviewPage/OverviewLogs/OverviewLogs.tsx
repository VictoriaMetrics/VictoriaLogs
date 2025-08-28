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

const operator = ExtraFilterOperator.Equals;

const OverviewLogs:FC = () => {
  const [searchParams] = useSearchParams();

  const { period, relativeTime, duration } = useTimeState();
  const { logs, isLoading, error, fetchLogs, abortController } = useFetchLogs();
  const { extraFilters } = useExtraFilters();
  const { fieldFilter, fieldValueFilter } = useFieldFilter();
  const { streamFieldFilter, streamFieldValueFilter } = useStreamFieldFilter();
  const copyToClipboard = useCopyToClipboard();

  const [copied, setCopied] = useState<boolean>(false);
  const [limit, setLimit] = useState(10);
  const hidePreviewLogs = useMemo(() => Boolean(searchParams.get("hide_logs")), [searchParams]);

  const query = useMemo(() => {
    const queryParts: string[] = [];

    if (streamFieldFilter) {
      const filterByStream = filterToExpr({ field: streamFieldFilter, value: streamFieldValueFilter || "*", operator });
      queryParts.push(filterByStream);
    }

    if (fieldFilter) {
      const filterByField = filterToExpr({ field: fieldFilter, value: fieldValueFilter || "*", operator });
      queryParts.push(filterByField);
    }

    if (extraFilters.length) {
      extraFilters.forEach(f => queryParts.push(filterToExpr(f)));
    }

    return queryParts.length ? queryParts.join("\n") : "*";
  }, [period, fieldFilter, fieldValueFilter, streamFieldFilter, streamFieldValueFilter, extraFilters]);

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
        <span className="vm-title">Preview logs:</span>
        <div className="vm-overview-logs-query">
          <p className="vm-overview-logs-query__expr">{query}</p>
          <div className="vm-overview-logs-query__limit">
            <SelectLimit
              label="&nbsp;|&nbsp;limit"
              limit={limit}
              onChange={setLimit}
            />
          </div>
        </div>
        <div className="vm-overview-logs-header__actions">
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
