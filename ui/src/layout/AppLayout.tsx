import React from "react";
import { Outlet } from "react-router-dom";
import { Box } from "@mui/material";
import Sidebar, { SIDEBAR_WIDTH } from "./Sidebar";

export default function AppLayout() {
  return (
    <Box sx={{ display: "flex", minHeight: "100vh" }}>
      <Sidebar />
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          bgcolor: "background.default",
          minHeight: "100vh",
          ml: `${SIDEBAR_WIDTH}px`,
        }}
      >
        <Outlet />
      </Box>
    </Box>
  );
}
