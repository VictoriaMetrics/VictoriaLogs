import { useSearchParams } from "react-router-dom";
import { useCallback, useEffect, useRef } from "preact/compat";

export const useQueryFilter = (param: string) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const value = searchParams.get(param) || "";

  const setValue = useCallback((newValue?: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      const currentValue = prev.get(param);

      if (newValue && newValue !== currentValue) {
        next.set(param, newValue);
      } else {
        next.delete(param);
      }

      return next;
    });
  }, [setSearchParams, param]);

  return { value, setValue };
};

export const useFieldFilter = () => {
  const { value: field, setValue: setField } = useQueryFilter("field");
  const { value: fieldValue, setValue: setFieldValue } = useQueryFilter("field_value");

  const isFirstRender = useRef(true);

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false; // Skip the first render
      return;
    }

    // Clear field value when field changes
    setFieldValue();
  }, [field]);

  return {
    fieldFilter: field,
    setFieldFilter: setField,
    fieldValueFilter: fieldValue,
    setFieldValueFilter: setFieldValue,
  };
};


export const useStreamFieldFilter = () => {
  const { value: streamField, setValue: setStreamField } = useQueryFilter("stream_field");
  const { value: streamFieldValue, setValue: setStreamFieldValue } = useQueryFilter("stream_field_value");

  const isFirstRender = useRef(true);

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false; // Skip the first render
      return;
    }

    // Clear stream field value when stream field changes
    setStreamFieldValue();
  }, [streamField]);

  return {
    streamFieldFilter: streamField,
    setStreamFieldFilter: setStreamField,
    streamFieldValueFilter: streamFieldValue,
    setStreamFieldValueFilter: setStreamFieldValue,
  };
};

