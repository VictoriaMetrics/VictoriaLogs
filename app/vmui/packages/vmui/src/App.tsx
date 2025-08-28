import { FC, useState } from "preact/compat";
import { HashRouter, Route, Routes } from "react-router-dom";
import AppContextProvider from "./contexts/AppContextProvider";
import ThemeProvider from "./components/Main/ThemeProvider/ThemeProvider";
import QueryPage from "./pages/QueryPage/QueryPage";
import LogsLayout from "./layouts/LogsLayout/LogsLayout";
import OverviewPage from "./pages/OverviewPage/OverviewPage";
import StreamContext from "./pages/StreamContext/StreamContext";
import router from "./router";
import "./constants/markedPlugins";
import PreviewIcons from "./components/Main/Icons/PreviewIcons";

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
                  element={<QueryPage/>}
                />
                <Route
                  path={router.overview}
                  element={<OverviewPage/>}
                />
                <Route
                  path={router.streamContext}
                  element={<StreamContext/>}
                />

                <Route
                  path={"/icons"}
                  element={<PreviewIcons/>}
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
