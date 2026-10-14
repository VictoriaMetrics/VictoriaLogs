import { Direction } from "./useFetchStreamContext";

interface UseStreamContextScroll {
  handleLoadMore: (dir: Direction) => void;
}

const SCROLL_THRESHOLD = 24;

export const useStreamContextScroll = ({ handleLoadMore }: UseStreamContextScroll) => {
  const handleScroll = (e: Event) => {
    const scrollContainer = e.currentTarget as HTMLDivElement | null;

    if (!scrollContainer) return;

    const { scrollTop, scrollHeight, clientHeight } = scrollContainer;

    const isTop = scrollTop <= SCROLL_THRESHOLD;
    const isBottom = scrollTop + clientHeight >= scrollHeight - SCROLL_THRESHOLD;

    if (isTop) handleLoadMore("before");
    if (isBottom) handleLoadMore("after");
  };

  return { handleScroll };
};
