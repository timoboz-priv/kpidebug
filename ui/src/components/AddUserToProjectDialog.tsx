import React, { useState } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Button,
  Alert,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
} from "@mui/material";
import { addMember, Role } from "../api/projects";

interface AddUserToProjectDialogProps {
  open: boolean;
  onClose: () => void;
  projectId: string;
  onMemberAdded: () => void;
}

export default function AddUserToProjectDialog({
  open,
  onClose,
  projectId,
  onMemberAdded,
}: AddUserToProjectDialogProps) {
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("read");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email.trim()) return;

    setLoading(true);
    setError(null);
    try {
      await addMember(projectId, { email: email.trim(), role });
      onMemberAdded();
      handleClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to add member");
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    setEmail("");
    setRole("read");
    setError(null);
    onClose();
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <form onSubmit={handleSubmit}>
        <DialogTitle>Add Member</DialogTitle>
        <DialogContent>
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          <TextField
            autoFocus
            fullWidth
            label="Email Address"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            margin="normal"
            required
            helperText="The user must have signed in at least once"
          />
          <FormControl fullWidth margin="normal">
            <InputLabel>Role</InputLabel>
            <Select value={role} label="Role" onChange={(e) => setRole(e.target.value as Role)}>
              <MenuItem value="read">Read</MenuItem>
              <MenuItem value="edit">Edit</MenuItem>
              <MenuItem value="admin">Admin</MenuItem>
            </Select>
          </FormControl>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} disabled={loading}>
            Cancel
          </Button>
          <Button type="submit" variant="contained" disabled={loading || !email.trim()}>
            Add Member
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}
