import { vmDate, VmDate } from "./vmDate";

export const isValidDate = (value: number | Date | VmDate | string): boolean => {
  return vmDate(value).isValid();
};
