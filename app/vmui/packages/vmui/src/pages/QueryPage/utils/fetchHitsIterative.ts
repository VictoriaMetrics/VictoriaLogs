import { LogHits } from "../../../api/types";
import { fetchHitsOnce, FetchHitsOptions } from "./fetchHitsOnce";
import { getHitsTimeRanges } from "./getHitsTimeRanges";
import { MutableRef } from "preact/hooks";

type FetchHitsIterativeOptions = FetchHitsOptions & {
  isPausedRef: MutableRef<boolean>
  onUpdateLoading: (hit: LogHits) => void;
  onUpdate: (hits: LogHits[], durationMs?: number) => void;
};

export const fetchHitsIterative = async ({
  isPausedRef,
  url,
  onUpdate,
  onUpdateLoading,
  ...init
}: FetchHitsIterativeOptions) => {
  const body = new URLSearchParams(init.body);

  const timeRanges = getHitsTimeRanges({
    start: body.get("start")!,
    end: body.get("end")!,
    step: body.get("step")!,
    offset: body.get("offset")!
  });

  for (const range of timeRanges) {
    while (isPausedRef.current) {
      init.signal.throwIfAborted();
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    init.signal.throwIfAborted();

    body.set("start", range.start);
    body.set("end", range.end);
    body.set("step", range.step);

    // Create a placeholder bar to indicate loading status.
    onUpdateLoading(createPlaceholderHit(range.start));

    const { hits, durationMs } = await fetchHitsOnce({ ...init, url, body });

    init.signal.throwIfAborted();

    onUpdate(hits, durationMs);
  }
};

const createPlaceholderHit = (timestamp: string): LogHits => {
  return {
    timestamps: [timestamp],
    values: [0],
    total: 0,
    fields: {},
    _isOther: false,
    _isLoading: true
  };
};
