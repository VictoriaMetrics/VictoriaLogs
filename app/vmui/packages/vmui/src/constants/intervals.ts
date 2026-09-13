import { getNanosecondsFromDuration } from "../utils/time";
import { IntervalOption } from "../types";

const MICROSECONDS = ["0.01ms", "0.025ms", "0.05ms", "0.1ms", "0.5ms"];
const MILLISECONDS = ["1ms", "5ms", "10ms", "25ms", "50ms", "100ms", "250ms", "500ms"];
const SECONDS = ["1s", "5s", "10s", "15s", "30s"];
const MINUTES = ["1m", "5m", "10m", "15m", "30m"];
const HOURS = ["1h", "3h", "6h", "12h"];
const DAYS = ["1d", "2d", "7d", "14d", "28d", "91d", "182d", "364d"];

export const ALLOWED_INTERVALS: IntervalOption[] = [
  ...MICROSECONDS,
  ...MILLISECONDS,
  ...SECONDS,
  ...MINUTES,
  ...HOURS,
  ...DAYS,
].map((v) => ({ duration: v, valueNs: getNanosecondsFromDuration(v) }));
