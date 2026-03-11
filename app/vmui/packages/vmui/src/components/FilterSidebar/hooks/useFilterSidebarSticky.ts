import { RefObject, useLayoutEffect, useState } from "react";

type StickyMetrics = { height: number; top: number };

export function useFilterSidebarSticky<T extends HTMLElement>(ref: RefObject<T>) {
  const [metrics, setMetrics] = useState<StickyMetrics>({ height: 0, top: 0 });

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;

    let rafId = 0;

    const measure = () => {
      const { paddingTop, paddingBottom } = getBodyPadding();
      const footerOverlap = getFooterOverlap();

      const elTop = el.getBoundingClientRect().top;
      const rectTop = Math.max(paddingTop, elTop);
      const overlapBottom = paddingBottom + footerOverlap;

      const nextHeight = Math.max(
        0,
        Math.round(window.innerHeight - rectTop - overlapBottom),
      );
      const nextTop = Math.round(paddingTop);

      setMetrics((prev) =>
        prev.height === nextHeight && prev.top === nextTop
          ? prev
          : { height: nextHeight, top: nextTop },
      );
    };

    const schedule = () => {
      if (rafId) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(measure);
    };

    window.addEventListener("scroll", schedule, { passive: true });
    window.addEventListener("resize", schedule);

    const footerEl = document.getElementById("vm-footer");
    const bodyEl = document.getElementById("vm-body");

    let ro: ResizeObserver | null = null;
    if (typeof ResizeObserver !== "undefined") {
      ro = new ResizeObserver(schedule);
      ro.observe(el);
      if (footerEl) ro.observe(footerEl);
      if (bodyEl) ro.observe(bodyEl);
    }

    measure();

    return () => {
      if (rafId) cancelAnimationFrame(rafId);
      window.removeEventListener("scroll", schedule);
      window.removeEventListener("resize", schedule);
      ro?.disconnect();
    };
  }, [ref]);

  return metrics;
}

const getFooterOverlap = () => {
  const footerEl = document.getElementById("vm-footer");
  if (!footerEl) return 0;
  const rect = footerEl.getBoundingClientRect();
  return Math.max(0, window.innerHeight - rect.top);
};

const getBodyPadding = () => {
  const bodyEl = document.getElementById("vm-body");
  if (!bodyEl) return { paddingTop: 0, paddingBottom: 0 };

  const bodyStyles = window.getComputedStyle(bodyEl);
  const paddingTop = parseFloat(bodyStyles.paddingTop) || 0;
  const paddingBottom = parseFloat(bodyStyles.paddingBottom) || 0;

  return { paddingTop, paddingBottom };
};
