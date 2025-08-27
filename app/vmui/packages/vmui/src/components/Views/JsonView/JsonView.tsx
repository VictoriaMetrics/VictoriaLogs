import { FC, useMemo } from "preact/compat";
import { DocumentVirtualizedList } from "../../Main/DocumentVirtualizedList/DocumentVirtualizedList";

interface Props {
  data: Record<string, string>[]
}

export const JsonView: FC<Props> = ({ data }) => {
  const jsonStr = useMemo(() => {
    return JSON.stringify(data, null, 2).split("\n");
  }, [data]);
  return (
    <div className="vm-json-view">
      <DocumentVirtualizedList data={jsonStr}/>
    </div>
  );
};
