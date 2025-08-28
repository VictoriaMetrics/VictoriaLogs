export enum ExtraFilterOperator {
  Equals = "=",
  NotEquals = "!=",
  Regex = "~",
  NotRegex = "!~",
}

export interface ExtraFilter {
  field: string;
  operator: ExtraFilterOperator;
  value: string;
}
