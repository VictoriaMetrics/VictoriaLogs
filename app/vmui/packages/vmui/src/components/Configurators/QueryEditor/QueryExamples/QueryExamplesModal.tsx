import { FC, useMemo, useState } from "preact/compat";
import { queryExamples } from "./examples";
import "./style.scss";
import TextField from "../../../Main/TextField/TextField";
import { SearchIcon, WikiIcon } from "../../../Main/Icons";
import classNames from "classnames";
import useDeviceDetect from "../../../../hooks/useDeviceDetect";
import QueryExamplesSidebar from "./QueryExamplesSidebar";
import QueryExamplesItem from "./QueryExamplesItem/QueryExamplesItem";
import { LOGS_DOCS_URL } from "../../../../constants/logs";
import Button from "../../../Main/Button/Button";

type Props = {
  onApply: (value: string) => void;
}

export const ALL_CATEGORIES = "All categories" as const;
type ExistingTitles = (typeof queryExamples)[number]["title"];
export type SectionTitle = ExistingTitles | typeof ALL_CATEGORIES;

const allSections = queryExamples.flatMap(s => s.examples);
const categories = [ALL_CATEGORIES, ...queryExamples.map(section => section.title)];

const QueryExamplesModal: FC<Props> = ({ onApply }) => {
  const { isMobile } = useDeviceDetect();

  const [selectedCategory, setSelectedCategory] = useState<SectionTitle>(ALL_CATEGORIES);
  const [search, setSearch] = useState("");

  const examples = useMemo(() => {
    if (isMobile || selectedCategory === ALL_CATEGORIES) return allSections;
    const target = queryExamples.find(section => section.title === selectedCategory);
    return target ? target.examples : [];
  }, [queryExamples, selectedCategory, isMobile]);

  const filteredExamples = useMemo(() => {
    const searchQuery = search.trim().toLowerCase();

    if (!search) return examples;
    return examples.filter(ex =>
      (ex.title.toLowerCase().includes(searchQuery)) ||
      (ex.description && ex.description.toLowerCase().includes(searchQuery)) ||
      (ex.pattern.toLowerCase().includes(searchQuery)) ||
      (ex.query.toLowerCase().includes(searchQuery))
    );
  }, [search, examples]);

  return (
    <div
      className={classNames({
      "vm-query-examples": true,
      "vm-query-examples__mobile": isMobile,
    })}
    >
      <div className="vm-query-examples-header">
        <div className="vm-query-examples-header-top">
          <h2 className="vm-query-examples-header__title">Browse examples or search to find the right query pattern.</h2>
          <Button
            as={"a"}
            target="_blank"
            href={`${LOGS_DOCS_URL}/logsql/`}
            rel="help noreferrer"
            variant="outlined"
            color="primary"
            startIcon={<WikiIcon/>}
          >
            LogsQL docs
          </Button>
        </div>
        <TextField
          autofocus
          placeholder="Filter examples..."
          startIcon={<SearchIcon/>}
          value={search}
          onChange={setSearch}
        />
      </div>

      {!isMobile && (
        <QueryExamplesSidebar
          categories={categories}
          value={selectedCategory}
          onChange={setSelectedCategory}
        />

      )}

      <div>
        <h3 className="vm-query-examples__title">{selectedCategory}</h3>
        <div className="vm-query-examples-content">
          {filteredExamples.map((example) => (
            <QueryExamplesItem
              key={example.title}
              example={example}
              onApply={onApply}
            />
        ))}

          {!filteredExamples.length && (
          <p className="vm-query-examples__no-results">No results found for {`"${search}"`}</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default QueryExamplesModal;
