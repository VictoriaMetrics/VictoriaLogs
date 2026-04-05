import { FC, memo } from "preact/compat";
import { Logs } from "../../../api/types";
import TreeField from "./TreeField";
import "./style.scss";

interface Props {
  log: Logs;
}

const TreeLogItem: FC<Props> = ({ log }) => {
  return (
    <div className="vm-tree-log-item">
      <TreeField
        fieldKey=""
        value={log}
        depth={0}
        path={[]}
        defaultExpanded={true}
      />
    </div>
  );
};

export default memo(TreeLogItem);
