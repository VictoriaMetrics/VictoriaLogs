import { FC, useEffect, useMemo } from "preact/compat";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useExtraFilters } from "../../hooks/useExtraFilters";
import { useState } from "react";
import { useFieldFilter, useStreamFieldFilter } from "../../hooks/useFieldFilter";
import { useFetchLogs } from "../../../QueryPage/hooks/useFetchLogs";
import { LogsFiledValues } from "../../../../api/types";
import { ExtraFilterOperator } from "../../FiltersBar/types";
import { fieldValuesCol, streamFieldValuesCol } from "../columns";
import OverviewTable from "../../OverviewTable/OverviewTable";
import "../../OverviewTable/style.scss";
import SelectLimit from "../../../../components/Main/Pagination/SelectLimit/SelectLimit";
import { buildFieldValuesQuery } from "./topFieldValuesUtils";
import { useOverviewState } from "../../../../state/overview/OverviewStateContext";
import useCopyToClipboard from "../../../../hooks/useCopyToClipboard";
import { CopyIcon, FilterIcon, FilterOffIcon, FocusIcon, UnfocusIcon } from "../../../../components/Main/Icons";
import TopRowMenu from "../FieldRowMenu/TopRowMenu";
import { altKeyLabel, ctrlKeyLabel } from "../../../../utils/keyboard";

const MODE_CONFIG = {
  top: {
    title: "Top N",
    description: "Most common values (highest hit counts)"
  },
  bottom: {
    title: "Bottom N",
    description: "Least common values (fewest hit counts)",
  }
};

export type ValuesMode = keyof typeof MODE_CONFIG; // "top" | "bottom"
const MODE_KEYS = Object.keys(MODE_CONFIG) as ValuesMode[]; // ["top","bottom"]

type Props = {
  scope: "field" | "stream";
}

const TopFieldValues: FC<Props> = ({ scope }) => {
  const { period } = useTimeState();
  const { logs, isLoading, error, fetchLogs, abortController } = useFetchLogs();
  const { extraParams, addNewFilter } = useExtraFilters();
  const { fieldFilter, fieldValueFilters, toggleFieldValueFilter } = useFieldFilter();
  const { streamFieldFilter, streamFieldValueFilters, toggleStreamFieldValueFilter } = useStreamFieldFilter();
  const { totalLogs } = useOverviewState();
  const copyToClipboard = useCopyToClipboard();

  const selectedKey = scope === "field" ? fieldFilter : streamFieldFilter;
  const selectedValue = scope === "field" ? fieldValueFilters : streamFieldValueFilters;
  const setterFilter = scope === "field" ? toggleFieldValueFilter : toggleStreamFieldValueFilter;

  const [mode, setMode] = useState(MODE_KEYS[0]);
  const [limit, setLimit] = useState(10);

  const rows: LogsFiledValues[] = useMemo(() => {
    return logs.map(l => {
      const hits = Number(l.hits) || 0;
      return {
        hits,
        value: l[selectedKey],
        percent: totalLogs > 0 ? (hits / totalLogs) * 100 : 0
      };
    });
  }, [selectedKey, logs, totalLogs]);

  const isEmptyList = (!isLoading && !error && (rows.length === 0)) || !selectedKey;
  const emptyText = selectedKey ? "No values found" : `Select ${scope === "field" ? "field" : "stream field"} name to see values`;

  const handleAddFilter = (row: LogsFiledValues, operator: ExtraFilterOperator) => {
    addNewFilter({ field: selectedKey, value: row.value, operator });
  };

  const selectFieldValue = (row: LogsFiledValues) => {
    setterFilter(row.value);
  };

  const handleCopy = async (row: LogsFiledValues) => {
    const copyValue = `${selectedKey}:${row.value}`;
    await copyToClipboard(copyValue, `\`${copyValue}\` has been copied`);
  };

  const handleClickRow = (row: LogsFiledValues, e: MouseEvent) => {
    const { ctrlKey, metaKey, altKey } = e;
    const ctrlMetaKey = ctrlKey || metaKey;

    if (ctrlMetaKey) {
      handleAddFilter(row, ExtraFilterOperator.NotEquals);
    } else if (altKey) {
      handleAddFilter(row, ExtraFilterOperator.Equals);
    } else {
      selectFieldValue(row);
    }
  };

  const detectActiveRow = (row: LogsFiledValues) => {
    return selectedValue.includes(row.value);
  };

  useEffect(() => {
    if (!selectedKey) return;
    const query = buildFieldValuesQuery(selectedKey, mode, limit);
    fetchLogs({ period, extraParams, limit, query });

    return () => abortController.abort();
  }, [period, extraParams.toString(), selectedKey, limit, mode]);

  const TableAction = (row: LogsFiledValues) => {
    const menu = [
      [{
        label: selectedValue.includes(row.value) ? "Unfocus" : "Focus",
        icon: selectedValue.includes(row.value) ? <UnfocusIcon/> : <FocusIcon/>,
        shortcut: "Click",
        onClick: () => selectFieldValue(row)
      }],
      [
        {
          label: "Include",
          icon: <FilterIcon/>,
          shortcut: `${altKeyLabel} + Click`,
          onClick: () => handleAddFilter(row, ExtraFilterOperator.Equals)
        },
        {
          label: "Exclude",
          icon: <FilterOffIcon/>,
          shortcut: `${ctrlKeyLabel} + Click`,
          onClick: () => handleAddFilter(row, ExtraFilterOperator.NotEquals)
        }
      ],
      [{
        label: "Copy",
        icon: <CopyIcon/>,
        onClick: () => handleCopy(row)
      }],
    ];
    return <TopRowMenu sections={menu}/>;
  };

  const TopFieldValuesHeaderControls = (
    <>
      <SelectLimit<ValuesMode>
        label="Mode"
        limit={mode}
        options={MODE_KEYS}
        onChange={(val) => setMode(val as ValuesMode)}
        renderOptionLabel={(v, isLabel) => (
          isLabel
            ? <span className="vm-top-fields-option__label">{v}</span>
            : (
              <div className="vm-top-fields-option">
                <span className="vm-top-fields-option__label">{v}</span>
                <span className="vm-top-fields-option__info">{MODE_CONFIG[v].description}</span>
              </div>
            )
        )}
      />
      <SelectLimit
        label={MODE_CONFIG[mode].title}
        limit={limit}
        onChange={setLimit}
      />
    </>
  );

  return (
    <OverviewTable
      enableSearch
      title={<>Field values: <b>`{selectedKey}`</b></>}
      rows={rows}
      columns={scope === "field" ? fieldValuesCol : streamFieldValuesCol}
      isLoading={isLoading}
      error={error}
      isEmptyList={isEmptyList}
      emptyListText={emptyText}
      onClickRow={handleClickRow}
      headerControls={TopFieldValuesHeaderControls}
      actionsRender={TableAction}
      detectActiveRow={detectActiveRow}
    />
  );
};

export default TopFieldValues;
