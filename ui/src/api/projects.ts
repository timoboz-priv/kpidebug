import apiClient from "./client";

export type Role = "read" | "edit" | "admin";

export interface Project {
  id: string;
  name: string;
  description: string;
}

export interface ProjectMember {
  user_id: string;
  role: Role;
  user_name: string;
  user_email: string;
}

export interface CreateProjectRequest {
  name: string;
  description?: string;
}

export interface AddMemberRequest {
  email: string;
  role?: Role;
}

export async function listProjects(): Promise<Project[]> {
  const response = await apiClient.get<Project[]>("/api/projects");
  return response.data;
}

export async function createProject(data: CreateProjectRequest): Promise<Project> {
  const response = await apiClient.post<Project>("/api/projects", data);
  return response.data;
}

export async function getProject(projectId: string): Promise<Project> {
  const response = await apiClient.get<Project>(`/api/projects/${projectId}`);
  return response.data;
}

export async function updateProject(projectId: string, data: Partial<CreateProjectRequest>): Promise<Project> {
  const response = await apiClient.put<Project>(`/api/projects/${projectId}`, data);
  return response.data;
}

export async function deleteProject(projectId: string): Promise<void> {
  await apiClient.delete(`/api/projects/${projectId}`);
}

export async function listMembers(projectId: string): Promise<ProjectMember[]> {
  const response = await apiClient.get<ProjectMember[]>(`/api/projects/${projectId}/members`);
  return response.data;
}

export async function addMember(projectId: string, data: AddMemberRequest): Promise<ProjectMember> {
  const response = await apiClient.post<ProjectMember>(`/api/projects/${projectId}/members`, data);
  return response.data;
}

export async function updateMemberRole(projectId: string, userId: string, role: Role): Promise<ProjectMember> {
  const response = await apiClient.put<ProjectMember>(`/api/projects/${projectId}/members/${userId}`, { role });
  return response.data;
}

export async function removeMember(projectId: string, userId: string): Promise<void> {
  await apiClient.delete(`/api/projects/${projectId}/members/${userId}`);
}

// --- Artifacts ---

export interface ProjectArtifact {
  id: string;
  project_id: string;
  type: "url" | "file";
  name: string;
  value: string;
  file_name: string;
  file_size: number;
  file_mime_type: string;
  created_at: string;
}

export async function listArtifacts(projectId: string): Promise<ProjectArtifact[]> {
  const response = await apiClient.get<ProjectArtifact[]>(
    `/api/projects/${projectId}/artifacts`,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function createUrlArtifact(
  projectId: string,
  url: string,
): Promise<ProjectArtifact> {
  const response = await apiClient.post<ProjectArtifact>(
    `/api/projects/${projectId}/artifacts/url`,
    { url },
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function createFileArtifact(
  projectId: string,
  file: File,
): Promise<ProjectArtifact> {
  const formData = new FormData();
  formData.append("file", file);
  const response = await apiClient.post<ProjectArtifact>(
    `/api/projects/${projectId}/artifacts/file`,
    formData,
    {
      headers: {
        "X-Project-Id": projectId,
        "Content-Type": "multipart/form-data",
      },
    },
  );
  return response.data;
}

export async function deleteArtifact(projectId: string, artifactId: string): Promise<void> {
  await apiClient.delete(
    `/api/projects/${projectId}/artifacts/${artifactId}`,
    { headers: { "X-Project-Id": projectId } },
  );
}
