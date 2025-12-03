import { useAppState } from "../state/common/StateContext";
import { useEffect, useState } from "preact/compat";
import { getBuildInfoUrl } from "../api/buildinfo";

const useGetVersion = () => {
  const { serverUrl } = useAppState();

  const [version, setVersion] = useState("");

  useEffect(() => {
    const fetchVersion = async () => {
      try {
        const response = await fetch(getBuildInfoUrl(serverUrl));
        const result = await response.json();
        setVersion(result?.data?.version);
      } catch (e) {
        console.error(e);
      }
    };

    fetchVersion();
  }, [serverUrl]);

  return { version };
};

export default useGetVersion;

