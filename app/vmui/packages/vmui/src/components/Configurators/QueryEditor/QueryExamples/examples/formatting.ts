import { QueryExamplesSection } from "../types";

export const formattingExamples: QueryExamplesSection = {
  title: "Formatting",
  examples: [
    {
      title: "Field Selection",
      pattern: "<q> | fields <f1>, ..., <fN>",
      query: "* | fields _time, _stream, _msg",
      description: "Display only specific log fields",
      docAnchor: "fields-pipe"
    },
    {
      title: "Field Formatting",
      pattern: "<q> | format \"<pattern>\" as <target>",
      query: "* | format \"request from <ip>:<port>\" as _msg",
      description: "Generate a new field based on a pattern",
      docAnchor: "format-pipe"
    },
    {
      title: "Field Rename",
      pattern: "<q> | rename <old> as <new>",
      query: "* | rename host as server",
      description: "Change field names in the output",
      docAnchor: "rename-pipe"
    },
    {
      title: "Field Deletion",
      pattern: "<q> | delete <f1>, ..., <fN>",
      query: "* | delete host, app",
      description: "Remove specific fields from the output",
      docAnchor: "delete-pipe"
    },
    {
      title: "Data Extraction",
      pattern: "<q> | extract \"<pattern>\" from <field>",
      query: "* | extract \"user_id=<id>\" from _msg",
      description: "Parse and extract structured fields from logs",
      docAnchor: "extract-pipe"
    },
  ],
};
