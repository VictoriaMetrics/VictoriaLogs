import { useCallback, useRef } from "preact/compat";
import uPlot, { Series } from "uplot";

type BarsLayout = {
  idx0: number;
  idx1: number;
  n: number;
  lefts: number[];
  widths: number[];
  valid: number[];
};

type HoverHit = {
  sidx: number;
  absIdx: number;
  j: number
};

const useBarPaths = () => {
  const layoutsRef = useRef<Map<number, BarsLayout>>(new Map());

  const ensureBarsLayout = (sidx: number, idx0: number, idx1Exclusive: number) => {
    const n = Math.max(0, idx1Exclusive - idx0);
    const map = layoutsRef.current;
    const cur = map.get(sidx);

    if (cur && cur.idx0 === idx0 && cur.idx1 === idx1Exclusive && cur.n === n) return;

    map.set(sidx, {
      idx0,
      idx1: idx1Exclusive,
      n,
      lefts: Array(n).fill(NaN),
      widths: Array(n).fill(NaN),
      valid: [],
    });
  };

  const barPaths = useCallback((u: uPlot, seriesIdx: number, idx0: number, idx1: number): Series.Paths | null => {
    if (seriesIdx === 0) return null;

    const idx1Excl = idx1 + 1;

    ensureBarsLayout(seriesIdx, idx0, idx1Excl);
    const layout = layoutsRef.current.get(seriesIdx)!;

    layout.valid.length = 0;

    const builder = uPlot?.paths?.bars?.({
      align: 1,
      gap: 1,
      size: [1],
      each: (_u, _sidx, idx, left, _top, width) => {
        const j = idx - layout.idx0;
        if (j < 0 || j >= layout.n) return;

        layout.lefts[j] = left;
        layout.widths[j] = width;

        if (Number.isFinite(left) && Number.isFinite(width)) {
          layout.valid.push(j);
        }
      },
    });

    return builder ? builder(u, seriesIdx, idx0, idx1) : null;
  }, []);

  const getPxRatio = (u: uPlot) => {
    const c = u.ctx.canvas;
    return c.clientWidth ? c.width / c.clientWidth : 1;
  };

  const findHoverHit = useCallback((u: uPlot): HoverHit | null => {
    const leftRelCss = u.cursor.left;
    if (leftRelCss == null) return null;

    const leftRel = leftRelCss * getPxRatio(u);

    if (leftRel < 0 || leftRel > u.bbox.width) return null;

    const cx = u.bbox.left + leftRel;

    for (let sidx = u.series.length - 1; sidx >= 1; sidx--) {
      if (!u.series[sidx]?.show) continue;

      const layout = layoutsRef.current.get(sidx);
      if (!layout || layout.valid.length === 0) continue;

      const v = layout.valid;

      let lo = 0;
      let hi = v.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (cx >= layout.lefts[v[mid]]) lo = mid + 1;
        else hi = mid;
      }

      const pos = lo - 1;
      if (pos < 0) continue;

      const j = v[pos];
      const left = layout.lefts[j];
      const width = layout.widths[j];

      if (!Number.isFinite(left) || !Number.isFinite(width)) continue;

      if (cx < left || cx >= left + width) continue;

      const absIdx = layout.idx0 + j;
      return { sidx, absIdx, j };
    }

    return null;
  }, []);

  const drawHoverBar = useCallback((u: uPlot) => {
    const hit = findHoverHit(u);
    if (!hit) return;

    let leftMin = Number.POSITIVE_INFINITY;
    let rightMax = Number.NEGATIVE_INFINITY;

    for (let sidx = 1; sidx < u.series.length; sidx++) {
      if (!u.series[sidx]?.show) continue;

      const layout = layoutsRef.current.get(sidx);
      if (!layout) continue;

      const j = hit.absIdx - layout.idx0;
      if (j < 0 || j >= layout.n) continue;

      const left = layout.lefts[j];
      const width = layout.widths[j];
      if (!Number.isFinite(left) || !Number.isFinite(width)) continue;

      leftMin = Math.min(leftMin, left);
      rightMax = Math.max(rightMax, left + width);
    }

    if (!Number.isFinite(leftMin) || !Number.isFinite(rightMax) || rightMax <= leftMin) return;

    const ctx = u.ctx;
    ctx.save();
    ctx.globalCompositeOperation = "source-atop";
    ctx.fillStyle = "rgba(255, 255, 255, 0.6)";
    ctx.fillRect(leftMin, u.bbox.top, rightMax - leftMin, u.bbox.height);
    ctx.restore();
  }, [findHoverHit]);

  const getHoverAbsIdxForBars = useCallback((u: uPlot) => {
    return findHoverHit(u)?.absIdx ?? -1;
  }, [findHoverHit]);

  return { barPaths, drawHoverBar, getHoverAbsIdxForBars };
};

export default useBarPaths;
