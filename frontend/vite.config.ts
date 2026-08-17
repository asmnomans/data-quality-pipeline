import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies /api -> the FastAPI backend (localhost:8000), so the
// frontend never needs CORS configured differently between dev and a future
// same-origin production deploy.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
});
