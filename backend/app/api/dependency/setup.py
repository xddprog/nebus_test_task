from dishka import AsyncContainer, make_async_container

from app.api.dependency.providers.app import AppProvider
from app.api.dependency.providers.request import RequestProvider


def setup_container() -> AsyncContainer:
    return make_async_container(AppProvider(), RequestProvider())
