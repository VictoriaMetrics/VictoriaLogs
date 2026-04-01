import { FC } from "preact/compat";
import CodeExample from "../../../../Main/CodeExample/CodeExample";
import QueryExamplesItemControls from "./QueryExamplesItemControls";
import { QueryExample } from "../types";

type Props = {
  example: QueryExample;
  onApply: (value: string) => void;
}

const QueryExamplesItem: FC<Props> = ({ example, onApply }) => {

  return (
    <div
      key={example.title}
      className="vm-query-examples-content-item"
    >
      <div className="vm-query-examples-content-item-header">
        <h3 className="vm-query-examples-content-item-header__title">
          {example.title}
        </h3>

        <QueryExamplesItemControls
          example={example}
          onApply={onApply}
        />

        <p className="vm-query-examples-content-item-header__description">
          {example.description}:
        </p>
      </div>

      <div className="vm-query-examples-content-item-code">
        <p className="vm-query-examples-content-item-code__label">Pattern:</p>
        <CodeExample code={example.pattern}/>
      </div>

      <div className="vm-query-examples-content-item-code">
        <p className="vm-query-examples-content-item-code__label">Example:</p>
        <CodeExample code={example.query}/>
      </div>
    </div>
  );
};

export default QueryExamplesItem;
