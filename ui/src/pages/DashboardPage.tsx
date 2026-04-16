import React from "react";
import { Box, Typography, Card, CardContent } from "@mui/material";
import { useProject } from "../contexts/ProjectContext";

export default function DashboardPage() {
  const { currentProject } = useProject();

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Dashboard
      </Typography>

      <Card>
        <CardContent>
          <Typography variant="h6">{currentProject?.name}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            {currentProject?.description || "No description"}
          </Typography>
        </CardContent>
      </Card>
    </Box>
  );
}
