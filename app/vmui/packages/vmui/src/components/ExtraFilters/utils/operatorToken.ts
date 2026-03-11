import { ExtraFilterOperator } from "../types";

const entries = [
  {
    operator: ExtraFilterOperator.Equals,
    token: "eq",
  },
  {
    operator: ExtraFilterOperator.NotEquals,
    token: "neq",
  },
  {
    operator: ExtraFilterOperator.Regex,
    token: "regex",
  },
  {
    operator: ExtraFilterOperator.NotRegex,
    token: "nregex",
  },
] as const;

type OperatorToken = (typeof entries)[number]["token"];

const operatorTokens = new Set<OperatorToken>(entries.map(({ token }) => token));

export const isExtraFilterToken = (value: unknown): value is OperatorToken => {
  return operatorTokens.has(value as OperatorToken);
};

export const extraFilterOperatorToToken = Object.fromEntries(
  entries.map(({ operator, token }) => [operator, token]),
) as Record<ExtraFilterOperator, OperatorToken>;

export const extraFilterTokenToOperator = Object.fromEntries(
  entries.map(({ operator, token }) => [token, operator]),
) as Record<OperatorToken, ExtraFilterOperator>;
