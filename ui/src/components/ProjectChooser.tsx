import React, { useState } from "react";
import {
  Box,
  Menu,
  MenuItem,
  ListItemText,
  ListItemIcon,
  Typography,
  ButtonBase,
  Divider,
} from "@mui/material";
import {
  Folder as FolderIcon,
  Check as CheckIcon,
  Add as AddIcon,
  UnfoldMore as UnfoldMoreIcon,
} from "@mui/icons-material";
import { useProject } from "../contexts/ProjectContext";
import { Project } from "../api/projects";
import CreateProjectDialog from "./CreateProjectDialog";

export default function ProjectChooser() {
  const { projects, currentProject, selectProject } = useProject();
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [createOpen, setCreateOpen] = useState(false);

  const handleOpen = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleSelect = (project: Project) => {
    selectProject(project);
    handleClose();
  };

  return (
    <>
      <ButtonBase
        onClick={handleOpen}
        sx={{
          width: "100%",
          display: "flex",
          alignItems: "center",
          gap: 1,
          px: 1.5,
          py: 1,
          borderRadius: 1,
          textAlign: "left",
          "&:hover": { bgcolor: "action.hover" },
        }}
      >
        <FolderIcon sx={{ fontSize: 20, color: "primary.main" }} />
        <Box sx={{ flex: 1, overflow: "hidden" }}>
          <Typography variant="body2" noWrap sx={{ fontWeight: 600 }}>
            {currentProject?.name || "Select project"}
          </Typography>
        </Box>
        <UnfoldMoreIcon sx={{ fontSize: 18, color: "text.secondary" }} />
      </ButtonBase>

      <Menu anchorEl={anchorEl} open={Boolean(anchorEl)} onClose={handleClose}>
        {projects.map((project) => (
          <MenuItem key={project.id} onClick={() => handleSelect(project)}>
            {currentProject?.id === project.id && (
              <ListItemIcon>
                <CheckIcon fontSize="small" />
              </ListItemIcon>
            )}
            <ListItemText inset={currentProject?.id !== project.id}>
              {project.name}
            </ListItemText>
          </MenuItem>
        ))}
        {projects.length > 0 && <Divider />}
        <MenuItem
          onClick={() => {
            handleClose();
            setCreateOpen(true);
          }}
        >
          <ListItemIcon>
            <AddIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>New project</ListItemText>
        </MenuItem>
      </Menu>

      <CreateProjectDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </>
  );
}
