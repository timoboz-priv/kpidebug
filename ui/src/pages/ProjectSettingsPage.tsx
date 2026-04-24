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
  Tabs,
  Tab,
  TextField,
  FormControl,
  InputLabel,
  Snackbar,
} from "@mui/material";
import {
  Delete as DeleteIcon,
  PersonAdd as PersonAddIcon,
  Add as AddIcon,
  Link as LinkIcon,
  InsertDriveFile as FileIcon,
} from "@mui/icons-material";
import { useProject } from "../contexts/ProjectContext";
import { useUser } from "../contexts/UserContext";
import {
  ProjectMember,
  ProjectArtifact,
  Role,
  listMembers,
  updateMemberRole,
  removeMember,
  updateProject,
  listArtifacts,
  createUrlArtifact,
  createFileArtifact,
  deleteArtifact,
} from "../api/projects";
import AddUserToProjectDialog from "../components/AddUserToProjectDialog";
import DataSourcesSection from "../components/DataSourcesSection";

export default function ProjectSettingsPage() {
  const { currentProject, selectProject } = useProject();
  const { user } = useUser();
  const [tab, setTab] = useState(0);
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
    } catch {
      setError("Failed to load members");
    } finally {
      setLoading(false);
    }
  }, [currentProject]);

  useEffect(() => { fetchMembers(); }, [fetchMembers]);

  if (!currentProject) return null;

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" sx={{ mb: 2 }}>Project Settings</Typography>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      <Box sx={{ borderBottom: 1, borderColor: "divider", mb: 3 }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)}>
          <Tab label="Settings" />
          <Tab label="Data Sources" />
          <Tab label="Access" />
        </Tabs>
      </Box>

      {tab === 0 && (
        <SettingsTab
          projectId={currentProject.id}
          initialName={currentProject.name}
          initialDescription={currentProject.description}
          isAdmin={isAdmin}
          onProjectUpdated={(name, description) => {
            selectProject({ ...currentProject, name, description });
          }}
        />
      )}

      {tab === 1 && (
        <Card>
          <CardContent>
            <DataSourcesSection projectId={currentProject.id} />
          </CardContent>
        </Card>
      )}

      {tab === 2 && (
        <AccessTab
          projectId={currentProject.id}
          members={members}
          loading={loading}
          isAdmin={isAdmin}
          currentUserId={user?.id || ""}
          onRoleChange={async (userId, role) => {
            try {
              await updateMemberRole(currentProject.id, userId, role);
              await fetchMembers();
            } catch (err: any) {
              setError(err.response?.data?.detail || "Failed to update role");
            }
          }}
          onRemove={async (userId) => {
            try {
              await removeMember(currentProject.id, userId);
              await fetchMembers();
            } catch (err: any) {
              setError(err.response?.data?.detail || "Failed to remove member");
            }
          }}
          onAddClick={() => setAddDialogOpen(true)}
        />
      )}

      <AddUserToProjectDialog
        open={addDialogOpen}
        onClose={() => setAddDialogOpen(false)}
        projectId={currentProject.id}
        onMemberAdded={fetchMembers}
      />
    </Box>
  );
}

// --- Settings Tab ---

function SettingsTab({
  projectId,
  initialName,
  initialDescription,
  isAdmin,
  onProjectUpdated,
}: {
  projectId: string;
  initialName: string;
  initialDescription: string;
  isAdmin: boolean;
  onProjectUpdated: (name: string, description: string) => void;
}) {
  const [name, setName] = useState(initialName);
  const [description, setDescription] = useState(initialDescription);
  const [saving, setSaving] = useState(false);
  const [snackbar, setSnackbar] = useState<string | null>(null);

  const hasChanges = name !== initialName || description !== initialDescription;

  const handleSave = async () => {
    setSaving(true);
    try {
      await updateProject(projectId, { name, description });
      onProjectUpdated(name, description);
      setSnackbar("Project updated");
    } catch {
      setSnackbar("Failed to update project");
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2 }}>Project Info</Typography>
          <TextField
            fullWidth
            label="Project Name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={!isAdmin}
            sx={{ mb: 2 }}
          />
          <TextField
            fullWidth
            label="Description"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            disabled={!isAdmin}
            multiline
            minRows={2}
            maxRows={4}
          />
          {isAdmin && (
            <Box sx={{ mt: 2, display: "flex", justifyContent: "flex-end" }}>
              <Button
                variant="contained"
                size="small"
                onClick={handleSave}
                disabled={!hasChanges || saving}
              >
                {saving ? "Saving..." : "Save"}
              </Button>
            </Box>
          )}
        </CardContent>
      </Card>

      <ArtifactsSection projectId={projectId} isAdmin={isAdmin} />

      <Snackbar
        open={snackbar !== null}
        autoHideDuration={2000}
        onClose={() => setSnackbar(null)}
        message={snackbar}
      />
    </>
  );
}

// --- Artifacts Section ---

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function ArtifactsSection({
  projectId,
  isAdmin,
}: {
  projectId: string;
  isAdmin: boolean;
}) {
  const [artifacts, setArtifacts] = useState<ProjectArtifact[]>([]);
  const [loading, setLoading] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [newType, setNewType] = useState<"url" | "file">("url");
  const [newUrl, setNewUrl] = useState("");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [saving, setSaving] = useState(false);

  const fetchArtifacts = useCallback(async () => {
    setLoading(true);
    try {
      const result = await listArtifacts(projectId);
      setArtifacts(result);
    } catch {
      // non-critical
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => { fetchArtifacts(); }, [fetchArtifacts]);

  const handleCreate = async () => {
    setSaving(true);
    try {
      if (newType === "url") {
        if (!newUrl.trim()) return;
        await createUrlArtifact(projectId, newUrl.trim());
      } else {
        if (!selectedFile) return;
        await createFileArtifact(projectId, selectedFile);
      }
      resetForm();
      await fetchArtifacts();
    } catch {
      // handled silently
    } finally {
      setSaving(false);
    }
  };

  const resetForm = () => {
    setShowForm(false);
    setNewUrl("");
    setSelectedFile(null);
    setNewType("url");
  };

  const handleDelete = async (artifactId: string) => {
    try {
      await deleteArtifact(projectId, artifactId);
      setArtifacts((prev) => prev.filter((a) => a.id !== artifactId));
    } catch {
      // handled silently
    }
  };

  const canSubmit = newType === "url" ? newUrl.trim().length > 0 : selectedFile !== null;

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1 }}>
          <Typography variant="h6">Artifacts</Typography>
          {!showForm && (
            <Button
              size="small"
              startIcon={<AddIcon />}
              onClick={() => setShowForm(true)}
            >
              Add
            </Button>
          )}
        </Box>

        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Background information about your project — URLs to docs, files, or other references.
        </Typography>

        {showForm && (
          <Box sx={{ mb: 2, p: 2, bgcolor: "action.hover", borderRadius: 1 }}>
            <Box sx={{ display: "flex", gap: 1, mb: 1.5 }}>
              <FormControl size="small" sx={{ minWidth: 100 }}>
                <InputLabel>Type</InputLabel>
                <Select
                  value={newType}
                  label="Type"
                  onChange={(e) => setNewType(e.target.value as "url" | "file")}
                >
                  <MenuItem value="url">URL</MenuItem>
                  <MenuItem value="file">File</MenuItem>
                </Select>
              </FormControl>

              {newType === "url" ? (
                <TextField
                  size="small"
                  label="URL"
                  value={newUrl}
                  onChange={(e) => setNewUrl(e.target.value)}
                  placeholder="https://..."
                  sx={{ flex: 1 }}
                />
              ) : (
                <Button
                  variant="outlined"
                  size="small"
                  component="label"
                  startIcon={<FileIcon />}
                  sx={{ flex: 1, justifyContent: "flex-start", textTransform: "none" }}
                >
                  {selectedFile ? selectedFile.name : "Choose file..."}
                  <input
                    type="file"
                    hidden
                    onChange={(e) => setSelectedFile(e.target.files?.[0] || null)}
                  />
                </Button>
              )}
            </Box>
            <Box sx={{ display: "flex", gap: 1, justifyContent: "flex-end" }}>
              <Button size="small" onClick={resetForm}>
                Cancel
              </Button>
              <Button
                size="small"
                variant="contained"
                onClick={handleCreate}
                disabled={saving || !canSubmit}
              >
                {saving ? "Adding..." : "Add"}
              </Button>
            </Box>
          </Box>
        )}

        {loading ? (
          <Box sx={{ display: "flex", justifyContent: "center", py: 2 }}>
            <CircularProgress size={20} />
          </Box>
        ) : artifacts.length === 0 ? (
          <Typography variant="body2" color="text.disabled">
            No artifacts yet.
          </Typography>
        ) : (
          <Box>
            {artifacts.map((artifact) => (
              <Box
                key={artifact.id}
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1.5,
                  py: 1,
                  borderBottom: 1,
                  borderColor: "divider",
                  "&:last-child": { borderBottom: 0 },
                }}
              >
                {artifact.type === "url" ? (
                  <LinkIcon fontSize="small" color="action" />
                ) : (
                  <FileIcon fontSize="small" color="action" />
                )}
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  {artifact.type === "url" ? (
                    <Typography variant="body2" noWrap>
                      {artifact.value}
                    </Typography>
                  ) : (
                    <>
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        {artifact.file_name}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {artifact.file_mime_type} &middot; {formatFileSize(artifact.file_size)}
                      </Typography>
                    </>
                  )}
                </Box>
                <Chip label={artifact.type} size="small" variant="outlined" />
                {isAdmin && (
                  <IconButton
                    size="small"
                    onClick={() => handleDelete(artifact.id)}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                )}
              </Box>
            ))}
          </Box>
        )}
      </CardContent>
    </Card>
  );
}

// --- Access Tab ---

function AccessTab({
  projectId,
  members,
  loading,
  isAdmin,
  currentUserId,
  onRoleChange,
  onRemove,
  onAddClick,
}: {
  projectId: string;
  members: ProjectMember[];
  loading: boolean;
  isAdmin: boolean;
  currentUserId: string;
  onRoleChange: (userId: string, role: Role) => void;
  onRemove: (userId: string) => void;
  onAddClick: () => void;
}) {
  const roleColor = (role: Role): "default" | "primary" | "error" => {
    switch (role) {
      case "admin": return "error";
      case "edit": return "primary";
      default: return "default";
    }
  };

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 2 }}>
          <Typography variant="h6">Members</Typography>
          {isAdmin && (
            <Button
              size="small"
              startIcon={<PersonAddIcon />}
              onClick={onAddClick}
            >
              Add Member
            </Button>
          )}
        </Box>

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
                      {isAdmin && member.user_id !== currentUserId ? (
                        <Select
                          size="small"
                          value={member.role}
                          onChange={(e) => onRoleChange(member.user_id, e.target.value as Role)}
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
                        {member.user_id !== currentUserId && (
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => onRemove(member.user_id)}
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
  );
}
