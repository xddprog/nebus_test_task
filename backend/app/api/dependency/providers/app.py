from typing import AsyncIterable

from aiohttp import ClientSession, ClientTimeout
from dishka import Provider, Scope, provide

from app.infrastructure.database.adapters.pg_connection import DatabaseConnection
from backend.app.infrastructure.broker.rabbit_broker import PaymentBroker


class AppProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_db_connection(self) -> AsyncIterable[DatabaseConnection]:
        connection = DatabaseConnection()
        try:
            yield connection
        finally:
            await connection.close()

    @provide(scope=Scope.APP)
    async def get_http_client(self) -> AsyncIterable[ClientSession]:
        async with ClientSession(timeout=ClientTimeout(total=10)) as client:
            yield client

    @provide(scope=Scope.APP)
    def get_broker(self) -> PaymentBroker:
        return PaymentBroker()
