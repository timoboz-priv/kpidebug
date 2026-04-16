import React from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { ThemeProvider, CssBaseline } from "@mui/material";
import theme from "./theme";
import { UserProvider, useUser } from "./contexts/UserContext";
import { ProjectProvider } from "./contexts/ProjectContext";
import ProtectedRoute from "./components/ProtectedRoute";
import AppLayout from "./layout/AppLayout";
import LoginPage from "./pages/LoginPage";
import DashboardPage from "./pages/DashboardPage";
import ProjectSettingsPage from "./pages/ProjectSettingsPage";

function AppRoutes() {
  const { firebaseUser, loading } = useUser();

  if (loading) return null;

  return (
    <Routes>
      <Route
        path="/login"
        element={firebaseUser ? <Navigate to="/" replace /> : <LoginPage />}
      />
      <Route
        element={
          <ProtectedRoute>
            <ProjectProvider>
              <AppLayout />
            </ProjectProvider>
          </ProtectedRoute>
        }
      >
        <Route path="/" element={<DashboardPage />} />
        <Route path="/settings" element={<ProjectSettingsPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <UserProvider>
          <AppRoutes />
        </UserProvider>
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;
