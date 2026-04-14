import { QueryExamplesSection } from "../types";

export const aggregationsExamples: QueryExamplesSection = {
  title: "Aggregations",
  examples: [
    {
      title: "Count by Field",
      pattern: "<q> | stats by (<field>) count() rows",
      query: "* | stats by (level) count() rows",
      description: "Count logs grouped by a specific field",
      docAnchor: "stats-pipe",
    },
    {
      title: "Top Values by Field",
      pattern: `<q> | stats by (<field>) count() as hits 
    | sort by (hits desc)`,
      query: `* | stats by (method) count() as hits 
  | sort by (hits desc)`,
      description: "Find the most frequent values for a field",
      docAnchor: "stats-pipe"
    },
    {
      title: "Numeric Buckets",
      pattern: "<q> | stats by (<field>:<interval>) count() rows",
      query: "* | stats by (request_size_bytes:10KB) count() rows",
      description: "Group numeric values into custom-sized ranges",
      docAnchor: "stats-by-field-buckets"
    },
    {
      title: "IP Subnets",
      pattern: "<q> | stats by (<ip_field>/<mask>) count() rows",
      query: "* | stats by (ip/24) count() rows",
      description: "Aggregate logs by IP address networks",
      docAnchor: "stats-by-ipv4-buckets"
    }
  ],
};
