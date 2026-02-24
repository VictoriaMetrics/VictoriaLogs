import { RefObject, useEffect, useState } from "preact/compat";

type Options = {
  rowsPerPage: number;
  containerRef: RefObject<HTMLElement>;
}

export const useTableLogsPaginate = ({ rowsPerPage, containerRef }: Options) => {
  const [page, setPage] = useState(1);

  const startOffset = (page - 1) * rowsPerPage;
  const endOffset =  page * rowsPerPage;
  const offset: [number, number] = [startOffset, endOffset];

  const onChangePage = (newPage: number) => {
    setPage(newPage);
    if (containerRef.current) {
      const y = containerRef.current.getBoundingClientRect().top + window.scrollY - 50;
      if (y < window.scrollY) window.scrollTo({ top: y });
    }
  };

  useEffect(() => {
    setPage(1);
  }, [rowsPerPage]);

  return {
    page,
    offset,
    onChangePage
  };
};
