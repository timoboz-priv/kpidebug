import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from google.cloud.firestore import Client as FirestoreClient

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.user_store_postgres import PostgresUserStore
from kpidebug.management.project_store_postgres import PostgresProjectStore
from kpidebug.data.data_store_postgres import PostgresDataStore
from kpidebug.management.types import Role


def migrate_users(firestore: FirestoreClient, pool_manager: ConnectionPoolManager) -> int:
    pool = pool_manager.pool()
    docs = firestore.collection("users").get()
    count = 0
    for doc in docs:
        data = doc.to_dict()
        with pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO users (id, name, email, avatar_url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    avatar_url = EXCLUDED.avatar_url
                """,
                (doc.id, data.get("name", ""), data.get("email", ""), data.get("avatar_url", "")),
            )
        count += 1
    return count


def migrate_projects(firestore: FirestoreClient, pool_manager: ConnectionPoolManager) -> tuple[int, int, list[str]]:
    pool = pool_manager.pool()
    docs = firestore.collection("projects").get()
    project_ids: list[str] = []
    project_count = 0
    member_count = 0

    for doc in docs:
        data = doc.to_dict()
        project_id = doc.id
        project_ids.append(project_id)

        with pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO projects (id, name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description
                """,
                (project_id, data.get("name", ""), data.get("description", "")),
            )
        project_count += 1

        member_docs = (
            firestore.collection("projects").document(project_id)
            .collection("members").get()
        )
        for member_doc in member_docs:
            mdata = member_doc.to_dict()
            with pool.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO project_members (project_id, user_id, role, user_name, user_email)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (project_id, user_id) DO UPDATE SET
                        role = EXCLUDED.role,
                        user_name = EXCLUDED.user_name,
                        user_email = EXCLUDED.user_email
                    """,
                    (
                        project_id,
                        mdata.get("user_id", ""),
                        mdata.get("role", Role.READ.value),
                        mdata.get("user_name", ""),
                        mdata.get("user_email", ""),
                    ),
                )
            member_count += 1

    return project_count, member_count, project_ids


def migrate_data_sources(
    firestore: FirestoreClient, pool_manager: ConnectionPoolManager,
    project_ids: list[str],
) -> int:
    pool = pool_manager.pool()
    source_count = 0

    for project_id in project_ids:
        creds_by_source: dict[str, dict] = {}
        cred_docs = (
            firestore.collection("projects").document(project_id)
            .collection("data_source_credentials").get()
        )
        for doc in cred_docs:
            creds_by_source[doc.id] = doc.to_dict()

        source_docs = (
            firestore.collection("projects").document(project_id)
            .collection("data_sources").get()
        )
        for doc in source_docs:
            data = doc.to_dict()
            dimensions_json = json.dumps(data.get("dimensions", []))
            credentials = data.get("credentials") or creds_by_source.get(doc.id, {})
            creds_json = json.dumps(credentials)
            with pool.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO data_sources (id, project_id, name, type, dimensions, credentials)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (project_id, id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        dimensions = EXCLUDED.dimensions,
                        credentials = EXCLUDED.credentials
                    """,
                    (doc.id, project_id, data.get("name", ""), data.get("type", "custom"), dimensions_json, creds_json),
                )
            source_count += 1

    return source_count


def main() -> None:
    pool_manager = ConnectionPoolManager()
    firestore = FirestoreClient()

    try:
        PostgresUserStore(pool_manager).ensure_tables()
        PostgresProjectStore(pool_manager).ensure_tables()
        PostgresDataStore(pool_manager).ensure_tables()

        user_count = migrate_users(firestore, pool_manager)
        print(f"Migrated {user_count} users.")

        project_count, member_count, project_ids = migrate_projects(firestore, pool_manager)
        print(f"Migrated {project_count} projects with {member_count} members.")

        source_count = migrate_data_sources(firestore, pool_manager, project_ids)
        print(f"Migrated {source_count} data sources.")
    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
