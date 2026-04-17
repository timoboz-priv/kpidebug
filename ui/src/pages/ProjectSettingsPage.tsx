import React, { useEffect, useState, useCallback } from "react";
import {
  Box,
  Typography,
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  IconButton,
  Button,
  Chip,
  Select,
  MenuItem,
  Alert,
  CircularProgress,
} from "@mui/material";
import { Delete as DeleteIcon, PersonAdd as PersonAddIcon } from "@mui/icons-material";
import { useProject } from "../contexts/ProjectContext";
import { useUser } from "../contexts/UserContext";
import { ProjectMember, Role, listMembers, updateMemberRole, removeMember } from "../api/projects";
import AddUserToProjectDialog from "../components/AddUserToProjectDialog";
import DataSourcesSection from "../components/DataSourcesSection";

export default function ProjectSettingsPage() {
  const { currentProject } = useProject();
  const { user } = useUser();
  const [members, setMembers] = useState<ProjectMember[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [addDialogOpen, setAddDialogOpen] = useState(false);

  const currentUserMember = members.find((m) => m.user_id === user?.id);
  const isAdmin = currentUserMember?.role === "admin";

  const fetchMembers = useCallback(async () => {
    if (!currentProject) return;
    setLoading(true);
    try {
      const result = await listMembers(currentProject.id);
      setMembers(result);
      setError(null);
    } catch (err: any) {
      setError("Failed to load members");
    } finally {
      setLoading(false);
    }
  }, [currentProject]);

  useEffect(() => {
    fetchMembers();
  }, [fetchMembers]);

  const handleRoleChange = async (userId: string, role: Role) => {
    if (!currentProject) return;
    try {
      await updateMemberRole(currentProject.id, userId, role);
      await fetchMembers();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to update role");
    }
  };

  const handleRemove = async (userId: string) => {
    if (!currentProject) return;
    try {
      await removeMember(currentProject.id, userId);
      await fetchMembers();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to remove member");
    }
  };

  if (!currentProject) return null;

  const roleColor = (role: Role): "default" | "primary" | "error" => {
    switch (role) {
      case "admin":
        return "error";
      case "edit":
        return "primary";
      default:
        return "default";
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 3 }}>
        <Typography variant="h4">Project Settings</Typography>
        {isAdmin && (
          <Button
            variant="contained"
            startIcon={<PersonAddIcon />}
            onClick={() => setAddDialogOpen(true)}
          >
            Add Member
          </Button>
        )}
      </Box>

      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6">{currentProject.name}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            {currentProject.description || "No description"}
          </Typography>
        </CardContent>
      </Card>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Members
          </Typography>
          {loading ? (
            <Box sx={{ display: "flex", justifyContent: "center", p: 3 }}>
              <CircularProgress />
            </Box>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Email</TableCell>
                    <TableCell>Role</TableCell>
                    {isAdmin && <TableCell align="right">Actions</TableCell>}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {members.map((member) => (
                    <TableRow key={member.user_id}>
                      <TableCell>{member.user_name || "—"}</TableCell>
                      <TableCell>{member.user_email}</TableCell>
                      <TableCell>
                        {isAdmin && member.user_id !== user?.id ? (
                          <Select
                            size="small"
                            value={member.role}
                            onChange={(e) =>
                              handleRoleChange(member.user_id, e.target.value as Role)
                            }
                          >
                            <MenuItem value="read">Read</MenuItem>
                            <MenuItem value="edit">Edit</MenuItem>
                            <MenuItem value="admin">Admin</MenuItem>
                          </Select>
                        ) : (
                          <Chip label={member.role} size="small" color={roleColor(member.role)} />
                        )}
                      </TableCell>
                      {isAdmin && (
                        <TableCell align="right">
                          {member.user_id !== user?.id && (
                            <IconButton
                              size="small"
                              color="error"
                              onClick={() => handleRemove(member.user_id)}
                            >
                              <DeleteIcon fontSize="small" />
                            </IconButton>
                          )}
                        </TableCell>
                      )}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </CardContent>
      </Card>

      <AddUserToProjectDialog
        open={addDialogOpen}
        onClose={() => setAddDialogOpen(false)}
        projectId={currentProject.id}
        onMemberAdded={fetchMembers}
      />

      <Card sx={{ mt: 3 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Data Sources
          </Typography>
          <DataSourcesSection projectId={currentProject.id} />
        </CardContent>
      </Card>
    </Box>
  );
}
