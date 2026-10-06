import asyncio
import logging
from contextlib import suppress
from uuid import UUID

from dishka_faststream import FromDishka, setup_dishka
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitRouter
from faststream.rabbit.annotations import RabbitMessage

from app.api.dependency.setup import setup_container
from app.core.services.outbox_service import OutboxService
from app.core.services.payment_service import PaymentService
from backend.app.infrastructure.broker.rabbit_broker import PaymentBroker


logger = logging.getLogger(__name__)
container = setup_container()
app = FastStream(logger=logger)
router = RabbitRouter()
publisher_task: asyncio.Task | None = None


@router.subscriber(PaymentBroker.PAYMENTS_QUEUE, ack_policy=AckPolicy.MANUAL)
async def process_payment(
    message: RabbitMessage,
    service: FromDishka[PaymentService],
) -> None:
    payment_id: UUID | None = None
    for attempt in range(3):
        try:
            payload = await message.decode()
            payment_id = UUID(payload["payment_id"])

            await service.process_payment(payment_id)

        except Exception:
            logger.exception(
                f"Payment processing failed: payment_id={payment_id}, "
                f"message_id={message.message_id}, attempt={attempt + 1}/3"
            )
            if attempt < 2:
                await asyncio.sleep(2**attempt)
        else:
            await message.ack()
            return

    await message.reject(requeue=False)
    logger.error(
        f"Payment message rejected after 3 attempts: payment_id={payment_id}, "
        f"message_id={message.message_id}, requeue=False"
    )


async def publish_outbox() -> None:
    while True:
        try:
            async with container() as scope:
                service = await scope.get(OutboxService)
                await service.publish_pending()
        except Exception:
            logger.exception("Outbox publication failed")
        await asyncio.sleep(1)


@app.on_startup
async def start_publisher() -> None:
    global publisher_task
    broker = await container.get(PaymentBroker)
    broker.include_router(router)
    setup_dishka(container, broker=broker, auto_inject=True)
    app.add_broker(broker)
    await broker.connect()
    await broker.declare_queues()
    publisher_task = asyncio.create_task(publish_outbox())


@app.on_shutdown
async def stop_publisher() -> None:
    if publisher_task is not None:
        publisher_task.cancel()
        with suppress(asyncio.CancelledError):
            await publisher_task


@app.after_shutdown
async def close_container() -> None:
    await container.close()
