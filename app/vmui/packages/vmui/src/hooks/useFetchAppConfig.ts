import { useAppDispatch, useAppState } from "../state/common/StateContext";
import { useEffect, useState } from "preact/compat";
import { ErrorTypes } from "../types";

const useFetchFlags = () => {
  const { serverUrl } = useAppState();
  const dispatch = useAppDispatch();

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ErrorTypes | string>("");

  useEffect(() => {
    const fetchAppConfig = async () => {
      setError("");
      setIsLoading(true);

      try {
        const data = await fetch(`${serverUrl}/select/vmui/config.json`);
        const config = await data.json();
        dispatch({ type: "SET_APP_CONFIG", payload: config || {} });
        const tenant = {
          accountID: data.headers.get("AccountID") || "",
          projectID: data.headers.get("ProjectID") || "",
          disableTenantInfo: data.headers.get("VL-Disable-Tenant-Controls") == "true",
        };
        dispatch({ type: "SET_TENANT_ID", payload: tenant });
      } catch (e) {
        setIsLoading(false);
        if (e instanceof Error) setError(`${e.name}: ${e.message}`);
      }
    };

    fetchAppConfig();
  }, [serverUrl]);

  return { isLoading, error };
};

export default useFetchFlags;

