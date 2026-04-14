export interface QueryExample {
  title: string;
  pattern: string;
  query: string;
  description?: string;
  docAnchor?: string;
}

export interface QueryExamplesSection {
  title: string;
  examples: QueryExample[];
}
