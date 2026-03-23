import { FC } from "preact/compat";
import classNames from "classnames";

type Props = {
  value: string | null;
  categories: string[];
  onChange: (value: string) => void;
}

const QueryExamplesSidebar: FC<Props> = ({ value, categories, onChange }) => {
  const handleChange = (nextValue: string) => () => {
    onChange(nextValue);
  };

  return (
    <div>
      <h3 className="vm-query-examples__title">Categories</h3>

      <div className="vm-query-examples-nav">
        {categories.map((category) => (
          <p
            key={category}
            className={classNames({
              "vm-query-examples-nav-item": true,
              "vm-query-examples-nav-item_active": value === category,
            })}
            onClick={handleChange(category)}
          >
            {category}
          </p>
        ))}
      </div>
    </div>
  );
};

export default QueryExamplesSidebar;
