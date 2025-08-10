import { FC, useState } from "preact/compat";
import { HashRouter, Route, Routes } from "react-router-dom";
import AppContextProvider from "./contexts/AppContextProvider";
import ThemeProvider from "./components/Main/ThemeProvider/ThemeProvider";
import ExploreLogs from "./pages/ExploreLogs/ExploreLogs";
import LogsLayout from "./layouts/LogsLayout/LogsLayout";
import StreamContext from "./pages/StreamContext/StreamContext";
import router from "./router";
import "./constants/markedPlugins";

const App: FC = () => {
  const [loadedTheme, setLoadedTheme] = useState(false);

  return <>
    <HashRouter>
      <AppContextProvider>
        <>
          <ThemeProvider onLoaded={setLoadedTheme}/>
          {loadedTheme && (
            <Routes>
              <Route
                path={"/"}
                element={<LogsLayout/>}
              >
                <Route
                  path={"/"}
                  element={<ExploreLogs/>}
                />
                <Route
                  path={router.streamContext}
                  element={<StreamContext/>}
                />
              </Route>
            </Routes>
          )}
        </>
      </AppContextProvider>
    </HashRouter>
  </>;
};

export default App;
