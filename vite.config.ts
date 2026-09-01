import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/static/dist/",
  plugins: [react()],
  build: {
    emptyOutDir: true,
    outDir: "src/overwatch/static/dist",
  },
});
