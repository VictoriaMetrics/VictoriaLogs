import { FC, ReactNode } from "preact/compat";
import { WITHOUT_GROUPING } from "../../../constants/logs";
import Accordion from "../../Main/Accordion/Accordion";

type Props = {
  groupBy: string;
  expand: boolean;
  onExpandChange: (expand: boolean) => void;
  header: ReactNode;
  children: ReactNode;
}

const GroupLogsItemWrapper: FC<Props> = ({ groupBy, expand, onExpandChange, header, children }) => {
  if (groupBy === WITHOUT_GROUPING) {
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
