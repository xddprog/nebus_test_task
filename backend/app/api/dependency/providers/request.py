from typing import AsyncIterable

from aiohttp import ClientSession
from dishka import Provider, Scope, provide
from fastapi import Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.repositories.outbox_repository import OutboxRepository
from app.core.repositories.payment_repository import PaymentRepository
from app.core.services.outbox_service import OutboxService
from app.core.services.payment_service import PaymentService
from app.infrastructure.config.config import APP_CONFIG
from app.infrastructure.database.adapters.pg_connection import DatabaseConnection
from backend.app.infrastructure.broker.rabbit_broker import PaymentBroker


class RequestProvider(Provider):
    @provide(scope=Scope.REQUEST)
    async def get_session(
        self, db_connection: DatabaseConnection
    ) -> AsyncIterable[AsyncSession]:
        async with await db_connection.get_session() as session:
            yield session

    @provide(scope=Scope.REQUEST)
    def get_payment_service(
        self, session: AsyncSession, http_client: ClientSession
    ) -> PaymentService:
        return PaymentService(
            repository=PaymentRepository(session),
            http_client=http_client,
        )

    @provide(scope=Scope.REQUEST)
    def get_outbox_service(
        self, session: AsyncSession, broker: PaymentBroker
    ) -> OutboxService:
        return OutboxService(repository=OutboxRepository(session), broker=broker)


async def require_api_key(
    api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    if api_key != APP_CONFIG.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
