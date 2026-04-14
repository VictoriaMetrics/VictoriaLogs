import { QueryExamplesSection } from "../types";

export const timeSeriesExamples: QueryExamplesSection = {
  title: "Time Series",
  examples: [
    {
      title: "Count by Time",
      pattern: "<q> | stats by (_time:<interval>) count() as total",
      query: "* | stats by (_time:1m) count() as total",
      description: "Calculate total log count for each time bucket",
      docAnchor: "stats-pipe"
    },
    {
      title: "Filtered Count by Time",
      pattern: `<filter> | stats by (_time:<interval>) count() rows 
         | sort by (_time)`,
      query: `error | stats by (_time:1h) count() rows 
      | sort by (_time)`,
      description: "Count specific occurrences grouped by time buckets",
      docAnchor: "stats-pipe"
    },
    {
      title: "Conditional Stats",
      pattern: `<q> | stats by (_time:<interval>) 
      count() if (<filter1>) as <r1>, 
      count() if (<filter2>) as <r2>`,
      query: `* | stats by (_time:5m) 
    count() if (error) as errors,
    count() if (warn) as warnings`,
      description: "Track multiple filters over time in a single query",
      docAnchor: "stats-with-additional-filters"
    },
    {
      title: "Grouped Time Series",
      pattern: "<q> | stats by (_time:<interval>, <field>) count() as hits",
      query: "* | stats by (_time:5m, host) count() as hits",
      description: "Create multiple series, one per unique field value",
      docAnchor: "stats-pipe"
    },
    {
      title: "Value Ratio by Time",
      pattern: `<q> | stats by (_time:<interval>) count() as <f1>,
      count() if (<filter>) as <f2> 
    | math <f2> / <f1>`,
      query: `* | stats by (_time:5m) count() as logs, 
    count() if (error) as errors 
  | math errors / logs`,
      description: "Calculate the ratio between two counts over time",
      docAnchor: "math-pipe"
    }
  ]
};
