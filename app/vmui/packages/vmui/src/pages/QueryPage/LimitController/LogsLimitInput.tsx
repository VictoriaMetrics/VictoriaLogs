import { FC, useCallback, useState } from "preact/compat";
import TextField from "../../../components/Main/TextField/TextField";
import { useEffect } from "react";
import { LOGS_MAX_LIMIT } from "../../../constants/logs";

type Props = {
  limit: number;
  onChangeLimit: (val: number) => void;
  onPressEnter: () => void;
  onError?: (error: boolean) => void;
}

const LogsLimitInput: FC<Props> = ({ limit, onChangeLimit, onPressEnter, onError }) => {
  const [errorLimit, setErrorLimit] = useState("");
  const [limitInput, setLimitInput] = useState(limit);

  const isValidLimit = (number: number) => {
    if (isNaN(number) || number <= 0) {
      setErrorLimit("Number must be > 0");
      return {
        isValid: false,
        errorMsg: "Number must be > 0"
      };
    } else if (number > LOGS_MAX_LIMIT) {
      return {
        isValid: false,
        errorMsg: `Max limit is ${LOGS_MAX_LIMIT.toLocaleString("en-US")}`
      };
    }

    return {
      isValid: true,
      errorMsg: ""
    };
  };

  const handleChangeLimit = useCallback((val: string) => {
    const number = +val;
    setLimitInput(number);
    const { isValid, errorMsg } = isValidLimit(number);
    if (!isValid) {
      setErrorLimit(errorMsg);
    } else {
      setErrorLimit("");
      onChangeLimit(number);
    }
  }, [onChangeLimit]);

  useEffect(() => {
    onError && onError(Boolean(errorLimit));
  }, [errorLimit]);

  useEffect(() => {
    const { isValid, errorMsg } = isValidLimit(limit);
    isValid ? setErrorLimit("") : setErrorLimit(errorMsg);
    setLimitInput(limit);
  }, [limit]);

  return (
    <TextField
      label="Log limits"
      type="number"
      value={limitInput}
      error={errorLimit}
      onChange={handleChangeLimit}
      onEnter={onPressEnter}
    />
  );
};

export default LogsLimitInput;
