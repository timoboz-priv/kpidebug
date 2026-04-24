import React from "react";
import { useNavigate, useLocation } from "react-router-dom";
import {
  Box,
  Drawer,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Typography,
  Avatar,
  Divider,
  IconButton,
  Tooltip,
} from "@mui/material";
import {
  Dashboard as DashboardIcon,
  Settings as SettingsIcon,
  Logout as LogoutIcon,
  AutoGraph as AutoGraphIcon,
  ShowChart as MetricsIcon,
  TableChart as DataIcon,
  Explore as ExploreIcon,
} from "@mui/icons-material";
import { useUser } from "../contexts/UserContext";
import ProjectChooser from "../components/ProjectChooser";

const SIDEBAR_WIDTH = 260;

interface NavItem {
  label: string;
  icon: React.ReactNode;
  path: string;
  children?: NavItem[];
}

const navItems: NavItem[] = [
  { label: "Dashboard", icon: <DashboardIcon />, path: "/" },
  {
    label: "Metrics",
    icon: <MetricsIcon />,
    path: "/metrics",
    children: [
      { label: "Explorer", icon: <ExploreIcon />, path: "/metrics/explorer" },
    ],
  },
  { label: "Data", icon: <DataIcon />, path: "/data" },
  { label: "Project Settings", icon: <SettingsIcon />, path: "/settings" },
];

export default function Sidebar() {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useUser();

  const handleLogout = async () => {
    await logout();
    navigate("/login");
  };

  return (
    <Drawer
      variant="permanent"
      sx={{
        width: SIDEBAR_WIDTH,
        flexShrink: 0,
        "& .MuiDrawer-paper": {
          width: SIDEBAR_WIDTH,
          boxSizing: "border-box",
          display: "flex",
          flexDirection: "column",
        },
      }}
    >
      {/* Logo */}
      <Box sx={{ p: 2, display: "flex", alignItems: "center", gap: 1.5 }}>
        <AutoGraphIcon sx={{ color: "primary.main", fontSize: 28 }} />
        <Typography variant="h6" color="primary" sx={{ fontWeight: 700 }}>
          KPI Debug
        </Typography>
      </Box>

      <Divider />

      {/* Project chooser */}
      <Box sx={{ px: 1, py: 1.5 }}>
        <ProjectChooser />
      </Box>

      <Divider />

      {/* Navigation */}
      <List sx={{ flex: 1, px: 1, py: 1 }}>
        {navItems.map((item) => {
          const isActive = item.children
            ? location.pathname === item.path
            : location.pathname === item.path;
          const isSectionActive = item.children
            ? location.pathname.startsWith(item.path)
            : location.pathname === item.path;

          return (
            <React.Fragment key={item.path}>
              <ListItemButton
                selected={isActive}
                onClick={() => navigate(item.path)}
                sx={{
                  borderRadius: 1,
                  mb: 0.5,
                  "&.Mui-selected": {
                    bgcolor: "primary.main",
                    color: "white",
                    "&:hover": { bgcolor: "primary.dark" },
                    "& .MuiListItemIcon-root": { color: "white" },
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 40 }}>{item.icon}</ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>

              {item.children && isSectionActive && item.children.map((child) => (
                <ListItemButton
                  key={child.path}
                  selected={location.pathname === child.path}
                  onClick={() => navigate(child.path)}
                  sx={{
                    borderRadius: 1,
                    mb: 0.5,
                    pl: 4,
                    py: 0.4,
                    "&.Mui-selected": {
                      bgcolor: "primary.main",
                      color: "white",
                      "&:hover": { bgcolor: "primary.dark" },
                      "& .MuiListItemIcon-root": { color: "white" },
                    },
                  }}
                >
                  <ListItemIcon sx={{ minWidth: 32 }}>{child.icon}</ListItemIcon>
                  <ListItemText
                    primary={child.label}
                    sx={{ "& .MuiListItemText-primary": { fontSize: "0.85rem" } }}
                  />
                </ListItemButton>
              ))}
            </React.Fragment>
          );
        })}
      </List>

      <Divider />

      {/* User card */}
      <Box sx={{ p: 2, display: "flex", alignItems: "center", gap: 1.5 }}>
        <Avatar
          src={user?.avatar_url || undefined}
          sx={{ width: 36, height: 36, bgcolor: "primary.light", fontSize: 14 }}
        >
          {user?.name?.charAt(0)?.toUpperCase() || "?"}
        </Avatar>
        <Box sx={{ flex: 1, overflow: "hidden" }}>
          <Typography variant="body2" noWrap sx={{ fontWeight: 600 }}>
            {user?.name || "User"}
          </Typography>
          <Typography variant="caption" color="text.secondary" noWrap>
            {user?.email || ""}
          </Typography>
        </Box>
        <Tooltip title="Sign out">
          <IconButton size="small" onClick={handleLogout}>
            <LogoutIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Box>
    </Drawer>
  );
}

export { SIDEBAR_WIDTH };
