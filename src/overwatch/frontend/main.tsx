import "@mantine/core/styles.css";
import "@mantine/charts/styles.css";
import "./hud.css";

import { createTheme, MantineProvider } from "@mantine/core";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";

const theme = createTheme({
  colors: {
    overwatch: [
      "#fff4e6",
      "#ffe8cc",
      "#ffd09b",
      "#ffb45c",
      "#fa9c1e",
      "#e9840b",
      "#cc6d00",
      "#a95800",
      "#874600",
      "#6d3700",
    ],
  },
  primaryColor: "overwatch",
  primaryShade: { light: 5, dark: 4 },
  defaultRadius: "xs",
  defaultGradient: { from: "overwatch.4", to: "overwatch.6", deg: 110 },
  fontFamily: 'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  headings: {
    fontFamily: '"Avenir Next Condensed", "Arial Narrow", Inter, sans-serif',
    fontWeight: "800",
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MantineProvider defaultColorScheme="auto" theme={theme}>
      <App />
    </MantineProvider>
  </StrictMode>,
);
