import React, { createContext, useContext, useEffect, useState, useCallback } from "react";
import { Project, listProjects } from "../api/projects";
import { useUser } from "./UserContext";

interface ProjectContextValue {
  projects: Project[];
  currentProject: Project | null;
  loading: boolean;
  error: string | null;
  selectProject: (project: Project) => void;
  refreshProjects: () => Promise<void>;
}

const ProjectContext = createContext<ProjectContextValue>({
  projects: [],
  currentProject: null,
  loading: false,
  error: null,
  selectProject: () => {},
  refreshProjects: async () => {},
});

const SELECTED_PROJECT_KEY = "kpidebug_selected_project_id";

export function ProjectProvider({ children }: { children: React.ReactNode }) {
  const { user } = useUser();
  const [projects, setProjects] = useState<Project[]>([]);
  const [currentProject, setCurrentProject] = useState<Project | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refreshProjects = useCallback(async () => {
    if (!user) return;
    setLoading(true);
    try {
      const projectList = await listProjects();
      setProjects(projectList);
      setError(null);

      // Restore previously selected project
      const savedId = localStorage.getItem(SELECTED_PROJECT_KEY);
      const savedProject = projectList.find((p) => p.id === savedId);
      if (savedProject) {
        setCurrentProject(savedProject);
      } else if (projectList.length > 0 && !currentProject) {
        setCurrentProject(projectList[0]);
      }
    } catch (err) {
      setError("Failed to load projects");
    } finally {
      setLoading(false);
    }
  }, [user]);

  useEffect(() => {
    refreshProjects();
  }, [refreshProjects]);

  const selectProject = (project: Project) => {
    setCurrentProject(project);
    localStorage.setItem(SELECTED_PROJECT_KEY, project.id);
  };

  return (
    <ProjectContext.Provider
      value={{ projects, currentProject, loading, error, selectProject, refreshProjects }}
    >
      {children}
    </ProjectContext.Provider>
  );
}

export function useProject(): ProjectContextValue {
  return useContext(ProjectContext);
}
