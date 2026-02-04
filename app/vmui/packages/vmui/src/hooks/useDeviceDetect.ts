import { useEffect, useState } from "preact/compat";
import { isMobileAgent } from "../utils/detect-device";
import useWindowSize from "./useWindowSize";

export const getIsMobile = () => {
  const mobileAgent = isMobileAgent();
  const smallWidth = window.innerWidth < 500;
  return mobileAgent || smallWidth;
};

export default function useDeviceDetect() {
  const windowSize = useWindowSize();
  const [isMobile, setMobile] = useState(getIsMobile());

  useEffect(() => {
    setMobile(getIsMobile());
  }, [windowSize]);

  return { isMobile };
}
