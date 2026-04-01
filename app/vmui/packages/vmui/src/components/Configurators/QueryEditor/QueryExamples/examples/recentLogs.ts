import { QueryExamplesSection } from "../types";

export const recentLogsExamples: QueryExamplesSection = {
  title: "Search",
  examples: [
    {
      title: "All Logs",
      pattern: "<q>",
      query: "*",
      description: "Show all available log entries",
      docAnchor: "any-value-filter"
    },
    {
      title: "Field Facets",
      pattern: "<q> | facets",
      query: "error | facets",
      description: "Show most frequent values for every log field",
      docAnchor: "facets-pipe"
    },
    {
      title: "Top Entries",
      pattern: "<q> | top <N> by (<fields>)",
      query: "* | top 10 by (_stream)",
      description: "Find the most frequent values for specific fields",
      docAnchor: "top-pipe",
    },
    {
      title: "First Entries",
      pattern: "<q> | first <N> by (<fields>)",
      query: "* | first 10 by (_time desc)",
      description: "Get the first N entries according to the sort order",
      docAnchor: "first-pipe"
    },
    {
      title: "Data Sorting",
      pattern: "<q> | sort by (<fields>)",
      query: "* | sort by (_time)",
      description: "Sort results by specific fields",
      docAnchor: "sort-pipe"
    },
    {
      title: "Simple Filter",
      pattern: "<word>",
      query: "error",
      description: "Find logs containing a specific keyword",
      docAnchor: "word-filter"
    },
    {
      title: "Stream Filter",
      pattern: "{<label>=\"<value>\"} <q>",
      query: "{app=\"nginx\"} error",
      description: "Filter logs by stream labels",
      docAnchor: "stream-filter"
    },
    {
      title: "Unique Values",
      pattern: "<q> | uniq by (<field>)",
      query: "* | uniq by (ip)",
      description: "Get unique values for a specific field",
      docAnchor: "uniq-by-pipe"
    },
  ],
};
