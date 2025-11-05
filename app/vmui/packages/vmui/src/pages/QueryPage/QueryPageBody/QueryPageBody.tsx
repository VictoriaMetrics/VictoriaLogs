import { FC, useRef } from "preact/compat";
import {
  CodeIcon,
  ListIcon,
  TableIcon,
  PlayIcon,
  VisibilityOffIcon,
  VisibilityIcon,
  DownloadIcon
} from "../../../components/Main/Icons";
import Tabs from "../../../components/Main/Tabs/Tabs";
import "./style.scss";
import classNames from "classnames";
import useDeviceDetect from "../../../hooks/useDeviceDetect";
import { Logs } from "../../../api/types";
import useStateSearchParams from "../../../hooks/useStateSearchParams";
import useSearchParamsFromObject from "../../../hooks/useSearchParamsFromObject";
import LineLoader from "../../../components/Main/LineLoader/LineLoader";
import GroupView from "../../../components/Views/GroupView/GroupView";
import TableView from "../../../components/Views/TableView/TableView";
import JsonLogsView from "../../../components/Views/JsonView/JsonLogsView";
import LiveTailingView from "../../../components/Views/LiveTailingView/LiveTailingView";
import Tooltip from "../../../components/Main/Tooltip/Tooltip";
import Button from "../../../components/Main/Button/Button";
import { useSearchParams } from "react-router-dom";
import Alert from "../../../components/Main/Alert/Alert";
import DownloadLogsModal from "../../../components/DownloadLogs/DownloadLogsModal";

interface Props {
  data: Logs[];
  queryParams?: Record<string, string>;
  isLoading: boolean;
  isPreview?: boolean;
}

enum DisplayType {
  group = "group",
  table = "table",
  json = "json",
  liveTailing = "liveTailing",
}

const tabs = [
  { label: "Group", value: DisplayType.group, icon: <ListIcon/>, Component: GroupView },
  { label: "Table", value: DisplayType.table, icon: <TableIcon/>, Component: TableView },
  { label: "JSON", value: DisplayType.json, icon: <CodeIcon/>, Component: JsonLogsView },
  { label: "Live", value: DisplayType.liveTailing, icon: <PlayIcon/>, Component: LiveTailingView },
];

const QueryPageBody: FC<Props> = ({ data, queryParams, isLoading, isPreview }) => {
  const { isMobile } = useDeviceDetect();
  const { setSearchParamsFromKeys } = useSearchParamsFromObject();
  const [activeTab, setActiveTab] = useStateSearchParams(DisplayType.group, "view");
  const settingsRef = useRef<HTMLDivElement>(null);

  const [searchParams, setSearchParams] = useSearchParams();
  const [hideLogs, setHideLogs] = useStateSearchParams(false, "hide_logs");

  const toggleHideLogs = () => {
    setHideLogs(prev => {
      const newVal = !prev;
      newVal ? searchParams.set("hide_logs", "true") : searchParams.delete("hide_logs");
      setSearchParams(searchParams);
      return newVal;
    });
  };

  const handleChangeTab = (view: string) => {
    setActiveTab(view as DisplayType);
    setSearchParamsFromKeys({ view });
  };

  const ActiveTabComponent = tabs.find(tab => tab.value === activeTab)?.Component;

  return (
    <div
      className={classNames({
        "vm-query-page-body": true,
        "vm-block": !isPreview,
        "vm-block_mobile": !isPreview && isMobile,
      })}
    >
      {isLoading && <LineLoader/>}
      <div
        className={classNames({
          "vm-query-page-body-header": true,
          "vm-section-header": true,
          "vm-query-page-body-header_mobile": isMobile,
        })}
      >
        <div
          className={classNames({
            "vm-section-header__tabs": true,
            "vm-query-page-body-header__tabs_mobile": isMobile,
          })}
        >
          <Tabs
            activeItem={String(activeTab)}
            items={tabs}
            onChange={handleChangeTab}
          />
          {activeTab !== DisplayType.liveTailing && (
            <div className="vm-query-page-body-header__log-info">
              Total logs returned: <b>{data.length}</b>
            </div>
          )}
        </div>
        <div
          className="vm-query-page-body-header__settings"
          ref={settingsRef}
        />
        <DownloadLogsModal queryParams={queryParams}>
          <Tooltip title="Download Logs">
            <Button
              variant="text"
              startIcon={<DownloadIcon/>}
              ariaLabel="Download Logs"
            />
          </Tooltip>
        </DownloadLogsModal>
        <Tooltip title={hideLogs ? "Show Logs" : "Hide Logs"}>
          <Button
            variant="text"
            color="primary"
            startIcon={hideLogs ? <VisibilityOffIcon/> : <VisibilityIcon/>}
            onClick={toggleHideLogs}
            ariaLabel="settings"
          />
        </Tooltip>
      </div>

      <div
        className={classNames({
          "vm-query-page-body__table": true,
          "vm-query-page-body__table_hide": hideLogs,
          "vm-query-page-body__table_mobile": isMobile,
        })}
      >
        {hideLogs && (
          <Alert variant="info">Logs are hidden. Updates paused.</Alert>
        )}

        {!hideLogs && ActiveTabComponent &&
          <ActiveTabComponent
            data={data}
            settingsRef={settingsRef}
          />
        }
      </div>
    </div>
  );
};

export default QueryPageBody;
