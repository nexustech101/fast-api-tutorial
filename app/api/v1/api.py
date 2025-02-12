from fastapi import APIRouter
from . import pipeline

api_router = APIRouter()

# Import and include other routers here
# from .endpoints import users, items
# api_router.include_router(users.router, prefix="/users", tags=["users"])
# api_router.include_router(items.router, prefix="/items", tags=["items"])

# Include pipeline router
api_router.include_router(pipeline.router, prefix="/pipeline", tags=["pipeline"])
