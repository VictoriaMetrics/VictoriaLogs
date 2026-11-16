import { DATE_TIME_FORMAT } from "../../../../constants/date";
import { vmDate } from "../../../../utils/time";

export const formatDateTimeInputValue = (value: string): string => {
  const date = vmDate(value);
  return date.isValid() ? date.nano().format(DATE_TIME_FORMAT) : value;
};

export const parseDateTimeInputValue = (value: string): string | null => {
  if (!value || !vmDate(value).isValid()) return null;

  try {
    const date = vmDate.tz(value);
    return date.isValid() ? date.nano().toISOString() : null;
  } catch {
    return null;
  }
};
