import dayjs, { ConfigType, Dayjs, OptionType } from "dayjs";
import { formatDateWithNanoseconds, getNanoTimestamp, toNanoPrecision } from "./nano";

export type VmDate = Dayjs;
export type VmDateInput = ConfigType;

export type VmDateInstance = VmDate & {
  raw: () => VmDateInput | undefined;
  nano: () => VmNanoDate;
};

export type VmNanoDate = {
  format: (format: string) => string;
  timestamp: () => bigint;
  toISOString: () => string;
};

type VmDateTimezone = {
  (date?: VmDateInput, timezone?: string): VmDateInstance;
  (date: VmDateInput, format: string, timezone?: string): VmDateInstance;
  guess: typeof dayjs.tz.guess;
  setDefault: typeof dayjs.tz.setDefault;
};

type VmDateFactory = {
  (date?: VmDateInput): VmDateInstance;
  (date?: VmDateInput, format?: OptionType, strict?: boolean): VmDateInstance;
  (date?: VmDateInput, format?: OptionType, locale?: string, strict?: boolean): VmDateInstance;
  tz: VmDateTimezone;
} & typeof dayjs;

type NanoSource = {
  value: string;
  raw?: string;
};

const getNanoSource = (date: Dayjs, source?: VmDateInput): NanoSource => {
  if (source === "" || !date.isValid()) return { value: "" };

  if (typeof source === "string") {
    return {
      value: date.toISOString(),
      raw: source,
    };
  }

  if (source && typeof (source as VmDateInstance).nano === "function") {
    return { value: (source as VmDateInstance).nano().toISOString() };
  }

  return { value: date.toISOString() };
};

const createVmNanoDate = (date: Dayjs, source?: VmDateInput): VmNanoDate => {
  const nanoSource = getNanoSource(date, source);
  const formatBase = (baseFormat: string): string => date.tz().format(baseFormat);
  const sourceValue = nanoSource.raw || nanoSource.value;

  return {
    format: (format: string) => formatDateWithNanoseconds(sourceValue, format, formatBase),
    timestamp: () => getNanoTimestamp(sourceValue, date.valueOf()),
    toISOString: () => toNanoPrecision(nanoSource.value, nanoSource.raw),
  };
};

const extendDate = (date: Dayjs, source?: VmDateInput): VmDateInstance => {
  Object.defineProperties(date, {
    raw: {
      value: () => source,
    },
    nano: {
      value: () => createVmNanoDate(date, source),
    },
  });

  return date as VmDateInstance;
};

const vmDateTz = ((date?: VmDateInput, formatOrTimezone?: string, timezone?: string) => {
  if (timezone !== undefined) {
    return extendDate(dayjs.tz(date, formatOrTimezone as string, timezone), date);
  }
  return extendDate(dayjs.tz(date, formatOrTimezone), date);
}) as VmDateTimezone;

vmDateTz.guess = dayjs.tz.guess;
vmDateTz.setDefault = dayjs.tz.setDefault;

export const vmDate = ((date?: VmDateInput, format?: OptionType, localeOrStrict?: string | boolean, strict?: boolean): VmDateInstance => {
  if (typeof localeOrStrict === "boolean") {
    return extendDate(dayjs(date, format, localeOrStrict), date);
  }

  return extendDate(dayjs(date, format, localeOrStrict, strict), date);
}) as VmDateFactory;

Object.getOwnPropertyNames(dayjs).forEach((propertyName) => {
  if (["length", "name", "prototype"].includes(propertyName)) return;

  const descriptor = Object.getOwnPropertyDescriptor(dayjs, propertyName);
  if (!descriptor) return;

  Object.defineProperty(vmDate, propertyName, descriptor);
});

vmDate.tz = vmDateTz;
