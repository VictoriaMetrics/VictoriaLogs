import { OrderDir } from "../../types";

export type FilterSidebarSortBy = "hits" | "name";

export type SortOptions = {
  by: FilterSidebarSortBy;
  direction: OrderDir;
};
