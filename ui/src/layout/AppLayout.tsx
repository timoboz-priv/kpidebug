import React, { useState } from "react";
import { Outlet } from "react-router-dom";
import {
  Box,
  Typography,
  Card,
  CardContent,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Button,
  Divider,
  CircularProgress,
} from "@mui/material";
import {
  Folder as FolderIcon,
  Add as AddIcon,
} from "@mui/icons-material";
import Sidebar, { SIDEBAR_WIDTH } from "./Sidebar";
import { useProject } from "../contexts/ProjectContext";
import CreateProjectDialog from "../components/CreateProjectDialog";

function ProjectGate() {
  const { projects, loading, selectProject } = useProject();
  const [createOpen, setCreateOpen] = useState(false);

  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "100vh",
        ml: `${SIDEBAR_WIDTH}px`,
        bgcolor: "background.default",
      }}
    >
      <Card sx={{ width: 400, maxWidth: "90%" }}>
        <CardContent sx={{ p: 3 }}>
          <Typography variant="h5" gutterBottom>
            Select a project
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Choose a project to get started.
          </Typography>

          {loading ? (
            <Box sx={{ display: "flex", justifyContent: "center", py: 3 }}>
              <CircularProgress />
            </Box>
          ) : projects.length > 0 ? (
            <List disablePadding>
              {projects.map((project) => (
                <ListItemButton
                  key={project.id}
                  onClick={() => selectProject(project)}
                  sx={{ borderRadius: 1 }}
                >
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    <FolderIcon color="primary" />
                  </ListItemIcon>
                  <ListItemText primary={project.name} />
                </ListItemButton>
              ))}
            </List>
          ) : (
            <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
              No projects yet.
            </Typography>
          )}

          <Divider sx={{ my: 2 }} />

          <Button
            fullWidth
            startIcon={<AddIcon />}
            onClick={() => setCreateOpen(true)}
          >
            Create new project
          </Button>
        </CardContent>
      </Card>

      <CreateProjectDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  );
}

export default function AppLayout() {
  const { currentProject, loading } = useProject();

  if (loading) {
    return (
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          minHeight: "100vh",
          bgcolor: "background.default",
        }}
      >
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", minHeight: "100vh" }}>
      <Sidebar />
      {!currentProject ? (
        <ProjectGate />
      ) : (
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
      )}
    </Box>
  );
}
