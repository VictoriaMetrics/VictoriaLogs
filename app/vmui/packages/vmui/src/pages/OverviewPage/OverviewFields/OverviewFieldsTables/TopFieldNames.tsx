import { FC, useEffect, useMemo } from "preact/compat";
import { useTimeState } from "../../../../state/time/TimeStateContext";
import { useFetchFieldNames } from "../../hooks/useFetchFieldNames";
import { useExtraFilters } from "../../hooks/useExtraFilters";
import { LogsFiledValues } from "../../../../api/types";
import { useFieldFilter } from "../../hooks/useFieldFilter";
import OverviewTable from "../../OverviewTable/OverviewTable";
import "../../OverviewTable/style.scss";
import { fieldNamesCol } from "../columns";
import { useOverviewState } from "../../../../state/overview/OverviewStateContext";
import { ExtraFilterOperator } from "../../FiltersBar/types";
import TopRowMenu from "../FieldRowMenu/TopRowMenu";
import { CopyIcon, FilterIcon, FilterOffIcon, FocusIcon, UnfocusIcon } from "../../../../components/Main/Icons";
import useCopyToClipboard from "../../../../hooks/useCopyToClipboard";
import { altKeyLabel, ctrlKeyLabel } from "../../../../utils/keyboard";

const TopFieldNames: FC = () => {
  const { period: { start, end } } = useTimeState();
  const { fetchFieldNames, fieldNames, loading, error } = useFetchFieldNames();
  const { extraParams, addNewFilter } = useExtraFilters();
  const { fieldFilter, setFieldFilter } = useFieldFilter();
  const { totalLogs } = useOverviewState();
  const copyToClipboard = useCopyToClipboard();

  const rows = useMemo(() => {
    return fieldNames.map((r) => {
      const percent = totalLogs > 0 ? (r.hits / totalLogs) * 100 : 0;
      return { ...r, percent };
    });
  }, [fieldNames, totalLogs]);

  const isEmptyList = !loading && !error && fieldNames.length === 0;

  const handleAddExcludeFilter = (row: LogsFiledValues) => {
    addNewFilter({ field: row.value, value: "*", operator: ExtraFilterOperator.NotEquals });
  };

  const handleAddIncludeFilter = (row: LogsFiledValues) => {
    addNewFilter({ field: row.value, value: "*", operator: ExtraFilterOperator.Equals });
  };

  const selectField = (row: LogsFiledValues) => {
    setFieldFilter(row.value);
  };

  const handleCopy = async (row: LogsFiledValues) => {
    const copyValue = row.value;
    await copyToClipboard(copyValue, `\`${copyValue}\` has been copied`);
  };

  const handleClickRow = (row: LogsFiledValues, e: MouseEvent) => {
    const { ctrlKey, metaKey, altKey } = e;
    const ctrlMetaKey = ctrlKey || metaKey;

    if (ctrlMetaKey) {
      handleAddExcludeFilter(row);
    } else if (altKey) {
      handleAddIncludeFilter(row);
    } else {
      selectField(row);
    }
  };

  const detectActiveRow = (row: LogsFiledValues) => {
    return row.value === fieldFilter;
  };

  useEffect(() => {
    fetchFieldNames({ start, end, extraParams });
  }, [start, end, extraParams.toString(), fetchFieldNames]);

  const TableAction = (row: LogsFiledValues) => {
    const menu = [
      [{
        label: fieldFilter === row.value ? "Unfocus" : "Focus",
        icon: fieldFilter === row.value ? <UnfocusIcon/> : <FocusIcon/>,
        shortcut: "Click",
        onClick: () => selectField(row)
      }],
      [
        {
          label: "Include",
          icon: <FilterIcon/>,
          shortcut: `${altKeyLabel} + Click`,
          onClick: () => handleAddIncludeFilter(row)
        },
        {
          label: "Exclude",
          icon: <FilterOffIcon/>,
          shortcut: `${ctrlKeyLabel} + Click`,
          onClick: () => handleAddExcludeFilter(row)
        },
      ],
      [{
        label: "Copy",
        icon: <CopyIcon/>,
        onClick: () => handleCopy(row)
      }],
    ];
    return <TopRowMenu sections={menu}/>;
  };

  return (
    <OverviewTable
      enableSearch
      title="Field names"
      rows={rows}
      columns={fieldNamesCol}
      isLoading={loading}
      error={error}
      isEmptyList={isEmptyList}
      emptyListText="No field names found"
      onClickRow={handleClickRow}
      detectActiveRow={detectActiveRow}
      actionsRender={TableAction}
    />
  );
};

export default TopFieldNames;
