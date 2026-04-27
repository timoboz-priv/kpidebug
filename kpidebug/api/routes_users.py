from fastapi import APIRouter, Depends

from kpidebug.api.auth import get_current_user
from kpidebug.api.stores import get_user_store
from kpidebug.management.user_store import AbstractUserStore
from kpidebug.management.types import User

router = APIRouter(prefix="/api/users", tags=["users"])


@router.get("/me")
def get_me(current_user: User = Depends(get_current_user)) -> User:
    return current_user


@router.put("/me")
def update_me(
    body: User,
    current_user: User = Depends(get_current_user),
    user_store: AbstractUserStore = Depends(get_user_store),
) -> User:
    return user_store.update(current_user.id, {
        "name": body.name,
        "avatar_url": body.avatar_url,
    })
