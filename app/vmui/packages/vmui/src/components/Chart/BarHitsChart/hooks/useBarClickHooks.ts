import dayjs from "dayjs";
import { TimePeriod } from "../../../../types";
import { useCallback, useEffect, useRef } from "preact/compat";

type Options = {
  getHoverAbsIdxForBars: (u: uPlot) => number;
  onBarClick: (period: TimePeriod) => void;
};

const useBarTimePeriodClickHooks = ({
  getHoverAbsIdxForBars,
  onBarClick,
}: Options) => {
  const handlersRef = useRef<WeakMap<uPlot, (e: MouseEvent) => void>>(new WeakMap());
  const getHoverRef = useRef(getHoverAbsIdxForBars);
  const onBarClickRef = useRef(onBarClick);

  useEffect(() => {
    getHoverRef.current = getHoverAbsIdxForBars;
    onBarClickRef.current = onBarClick;
  }, [getHoverAbsIdxForBars, onBarClick]);

  const makeHandler = useCallback((u: uPlot) => (_e: MouseEvent) => {
    const timestamps = u.data[0] as number[] | undefined;
    if (!timestamps || timestamps.length < 2) return;

    const step = timestamps[1] - timestamps[0];
    if (!Number.isFinite(step) || step <= 0) return;

    const absIdx = getHoverRef.current(u);
    if (absIdx < 0 || absIdx >= timestamps.length) return;

    const fromTs = timestamps[absIdx];
    if (!Number.isFinite(fromTs)) return;

    const toTs = fromTs + step;

    onBarClickRef.current({
      from: dayjs(fromTs * 1000).toDate(),
      to: dayjs(toTs * 1000).toDate(),
    });
  }, []);

  const destroy = useCallback((u: uPlot) => {
    const handler = handlersRef.current.get(u);
    if (!handler) return;
    u.over.removeEventListener("click", handler);
    handlersRef.current.delete(u);
  }, []);

  const ready = useCallback((u: uPlot) => {
    destroy(u);
    const handler = makeHandler(u);
    handlersRef.current.set(u, handler);
    u.over.addEventListener("click", handler);
  }, [destroy, makeHandler]);

  return { ready, destroy };
};

export default useBarTimePeriodClickHooks;
