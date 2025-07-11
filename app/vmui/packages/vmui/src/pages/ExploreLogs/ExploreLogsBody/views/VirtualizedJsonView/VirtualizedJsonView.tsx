import { FC, useMemo } from "preact/compat";
import "./style.scss";
import { DocumentVirtualizedList } from "./DocumentVirtualizedList";

interface Props {
  data: Record<string, string>[]
}

export const VirtualizedJsonView: FC<Props> = ({ data }) => {
  const jsonStr = useMemo(() => {
    return JSON.stringify(data, null, 2).split("\n");
  }, [data]);
  return (
    <div className="vm-json-view">
      <DocumentVirtualizedList data={jsonStr}/>
    </div>
  );
};
