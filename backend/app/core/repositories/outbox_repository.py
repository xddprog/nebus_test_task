from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.repositories.base_repository import SqlAlchemyRepository
from app.infrastructure.database.models.outbox import Outbox


class OutboxRepository(SqlAlchemyRepository[Outbox]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Outbox)

    async def get_pending(self) -> list[Outbox]:
        query = (
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .with_for_update(skip_locked=True)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def mark_published(self, event_ids: list[UUID]) -> None:
        query = (
            update(Outbox)
            .where(Outbox.id.in_(event_ids))
            .values(published_at=datetime.now(UTC))
        )
        await self.session.execute(query)
        await self.session.commit()
