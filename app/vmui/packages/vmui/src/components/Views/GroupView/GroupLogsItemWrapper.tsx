import { FC, ReactNode } from "preact/compat";
import Accordion from "../../Main/Accordion/Accordion";

type Props = {
  groupByFields: string[];
  expand: boolean;
  onExpandChange: (expand: boolean) => void;
  header: ReactNode;
  children: ReactNode;
}

const GroupLogsItemWrapper: FC<Props> = ({ groupByFields, expand, onExpandChange, header, children }) => {
  if (!groupByFields.length) {
    return (
      <div className="vm-group-logs-section__ungrouped">
        {children}
      </div>
    );
  }

  return (
    <Accordion
      defaultExpanded={expand}
      onChange={onExpandChange}
      title={header}
    >
      {children}
    </Accordion>
  );
};

export default GroupLogsItemWrapper;
