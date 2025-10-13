import { FC, memo } from "preact/compat";
import GroupLogs from "./GroupLogs";
import { ViewProps } from "../../../pages/QueryPage/QueryPageBody/types";
import EmptyLogs from "../../EmptyLogs/EmptyLogs";

const MemoizedGroupLogs = memo(GroupLogs);

const GroupView: FC<ViewProps> = ({ data, settingsRef }) => {
  if (!data.length) return <EmptyLogs />;

  return (
    <>
      <MemoizedGroupLogs
        logs={data}
        settingsRef={settingsRef}
      />
    </>
  );
};

export default GroupView;
