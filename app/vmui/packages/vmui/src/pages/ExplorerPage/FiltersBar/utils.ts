import { ExtraFilter, ExtraFilterOperator } from "./types";

export const getNewExtraFilter = (): ExtraFilter => ({
  field: "",
  operator: ExtraFilterOperator.Equals,
  value: "",
});

