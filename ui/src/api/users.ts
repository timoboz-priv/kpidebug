import apiClient from "./client";

export interface AppUser {
  id: string;
  name: string;
  email: string;
  avatar_url: string;
}

export interface UpdateUserRequest {
  name?: string;
  avatar_url?: string;
}

export async function getMe(): Promise<AppUser> {
  const response = await apiClient.get<AppUser>("/api/users/me");
  return response.data;
}

export async function updateMe(data: UpdateUserRequest): Promise<AppUser> {
  const response = await apiClient.put<AppUser>("/api/users/me", data);
  return response.data;
}
