from contextlib import asynccontextmanager

from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from app.api.dependency.setup import setup_container
from app.api.v1.routers import api_v1_router


container = setup_container()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        yield
    finally:
        await container.close()


app = FastAPI(lifespan=lifespan)
setup_dishka(container, app)
app.include_router(api_v1_router)
