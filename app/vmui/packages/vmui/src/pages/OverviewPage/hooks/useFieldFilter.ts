import { useSearchParams } from "react-router-dom";
import { useCallback, useEffect, useRef } from "preact/compat";

export enum UrlFieldFilterParam {
  Field = "field",
  FieldValue = "field_value",
  StreamField = "stream_field",
  StreamFieldValue = "stream_field_value",
}

const useQueryFilter = (param: string) => {
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

const useQueryFilterArray = (param: string) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const value = searchParams.getAll(param);

  const toggleValue = useCallback((newValue: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      const set = new Set(prev.getAll(param));

      if (set.has(newValue)) set.delete(newValue);
      else if (newValue) set.add(newValue);

      next.delete(param);
      for (const v of set) next.append(param, v);
      return next;
    });
  }, [setSearchParams, param]);

  const clear = useCallback(() => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      next.delete(param);
      return next;
    });
  }, [setSearchParams, param]);

  return { value, toggleValue, clear };
};

export const useFieldFilter = () => {
  const { value: field, setValue: setField } = useQueryFilter(UrlFieldFilterParam.Field);
  const { value: values, toggleValue, clear: clearFieldValues } = useQueryFilterArray(UrlFieldFilterParam.FieldValue);

  const isFirstRender = useRef(true);

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false; // Skip the first render
      return;
    }

    // Clear field value when the field changes
    clearFieldValues();
  }, [field]);

  return {
    fieldFilter: field,
    setFieldFilter: setField,
    fieldValueFilters: values,
    toggleFieldValueFilter: toggleValue,
  };
};


export const useStreamFieldFilter = () => {
  const { value: streamField, setValue: setStreamField } = useQueryFilter(UrlFieldFilterParam.StreamField);
  const { value: streamValues, toggleValue, clear: clearStreamFieldValues } = useQueryFilterArray(UrlFieldFilterParam.StreamFieldValue);

  const isFirstRender = useRef(true);

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false; // Skip the first render
      return;
    }

    // Clear stream field value when stream field changes
    clearStreamFieldValues();
  }, [streamField]);

  return {
    streamFieldFilter: streamField,
    setStreamFieldFilter: setStreamField,
    streamFieldValueFilters: streamValues,
    toggleStreamFieldValueFilter: toggleValue,
  };
};

