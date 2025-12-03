from fastapi import APIRouter, HTTPException, Depends
from typing import Annotated
from app.dependencies import get_db_session
from sqlalchemy.orm import Session
from app import crud


router = APIRouter(prefix="/users", tags=["Users"])


@router.get("/get_all_users")
def get_all_users(
    db_session: Annotated[Session, Depends(get_db_session)],
) -> dict:
    """Get all users from the database."""
    try:
        users = crud.get_all_users(db_session=db_session)
        # Convert each user to a dict (adjust fields as needed)
        users_dicts = [
            {
                "user_id": user["user_id"],
                "user_name": user["user_name"],
                # add other fields as needed
            }
            for user in users
        ]
        return {"Users": users_dicts}
    except Exception as e:
        print(f"Error getting list of users: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting list of users: {str(e)}",
        )


@router.post("/add_user")
def add_users(
    db_session: Annotated[Session, Depends(get_db_session)],
    name: str = None,
) -> dict:
    """Add or update user in the database."""
    try:
        return crud.upsert_user(db_session=db_session, name=name)
    except Exception as e:
        print(f"Error storing user: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error storing user: {str(e)}",
        )
