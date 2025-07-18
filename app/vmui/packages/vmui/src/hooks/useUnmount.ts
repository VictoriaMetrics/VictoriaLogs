import { useEffect, useRef } from "preact/compat";

export function useUnmount(fn: () => void) {
  const fnRef = useRef(fn);

  useEffect(() => {
    fnRef.current = fn;
  }, [fn]);

  useEffect(() => {
    return () => {
      fnRef.current();
    };
  }, []);
}
