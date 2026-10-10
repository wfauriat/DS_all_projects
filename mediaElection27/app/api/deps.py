"""Common FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_async_sessionmaker


async def get_session() -> AsyncIterator[AsyncSession]:
    sm = get_async_sessionmaker()
    async with sm() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
