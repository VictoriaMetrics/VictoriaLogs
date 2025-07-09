import { FC, useMemo } from "preact/compat";
import "./style.scss";
import { DocumentVirtualizedList } from "./DocumentVirtualizedList";
import { parseDataToJsonArray } from "./utils";

interface Props {
  data: Record<string, string>[]
}

export const VirtualizedJsonView: FC<Props> = ({ data }) => {
  const jsonStr = useMemo(() => {
    return parseDataToJsonArray(data);
  }, [data]);
  return (
    <div className="vm-json-view">
      <DocumentVirtualizedList data={jsonStr}/>
    </div>
  );
};
