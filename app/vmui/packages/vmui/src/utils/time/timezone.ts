import timezones from "../../constants/timezones";
import { getFromStorage } from "../storage";
import { Timezone } from "../../types";
import { vmDate } from "./vmDate";

const supportedValuesOf = Intl.supportedValuesOf;

export const supportedTimezones = supportedValuesOf ? supportedValuesOf("timeZone") as string[] : timezones;

export const setTimezone = (timezone: string) => {
  vmDate.tz.setDefault(timezone);
};

const isValidTimezone = (timezone: string) => {
  try {
    vmDate().tz(timezone);
    return true;
  } catch (e) {
    return false;
  }
};

export const getBrowserTimezone = () => {
  const timezone = vmDate.tz.guess();
  const isValid = isValidTimezone(timezone);
  return  {
    isValid,
    title: isValid ? `Browser Time (${timezone})` : "Browser timezone (UTC)",
    region: isValid ? timezone : "UTC",
  };
};

export const getUTCByTimezone = (timezone: string) => {
  const date = vmDate().tz(timezone);
  return `UTC${date.format("Z")}`;
};

export const getTimezoneList = (search = "") => {
  const normalizedSearch = search.toLowerCase();

  return supportedTimezones.reduce((acc: {[key: string]: Timezone[]}, region) => {
    const zone = (region.match(/^(.*?)\//) || [])[1] || "unknown";
    const utc = getUTCByTimezone(region);
    const utcForSearch = utc.replace(/UTC|0/g, "");
    const regionForSearch = region.replace(/[/_]/g, " ");
    const item = {
      region,
      utc,
      search: `${region} ${utc} ${regionForSearch} ${utcForSearch}`
    };

    const includeZone = !normalizedSearch || item.search.toLowerCase().includes(normalizedSearch);

    if (includeZone && acc[zone]) {
      acc[zone].push(item);
    } else if (includeZone) {
      acc[zone] = [item];
    }

    return acc;
  }, {});
};

export const initTimezone = () => {
  const storedTimezone = getFromStorage("TIMEZONE") as string;
  const isValidStorageTimezone = !!storedTimezone && isValidTimezone(storedTimezone);
  const timezone = isValidStorageTimezone ? storedTimezone : getBrowserTimezone().region;
  setTimezone(timezone);
  return timezone;
};

export const getNowInTimezone = () => vmDate.tz();

export const getDefaultTimezoneOffsetMinutes = (): number => {
  return getNowInTimezone().utcOffset();
};
