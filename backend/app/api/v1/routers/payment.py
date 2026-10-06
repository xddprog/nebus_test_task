from typing import Annotated
from uuid import UUID

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Header, status

from app.core.dto.payment import (
    CreatePaymentSchema,
    PaymentAcceptedSchema,
    PaymentSchema,
)
from app.core.services.payment_service import PaymentService


router = APIRouter(prefix="/payments", tags=["Payments"])


@router.post("", status_code=status.HTTP_202_ACCEPTED)
@inject
async def create_payment(
    data: CreatePaymentSchema,
    service: FromDishka[PaymentService],
    idempotency_key: Annotated[str, Header()],
) -> PaymentAcceptedSchema:
    return await service.create_payment(data, idempotency_key)


@router.get("/{payment_id}")
@inject
async def get_payment(
    payment_id: UUID,
    service: FromDishka[PaymentService],
) -> PaymentSchema:
    return await service.get_payment(payment_id)
