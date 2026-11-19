"""Coordinate downloads and deletion in the single-process ASGI controller."""
import asyncio
import re


class ObjectAccess:
    def __init__(self):
        self.gate = asyncio.Lock()
        self.idle = asyncio.Event()
        self.idle.set()
        self.readers = 0
        self.requests = 0


class DownloadGuard:
    path = re.compile(r"/api/v1/objects/([A-Za-z0-9_-]+)")

    def __init__(self, app):
        self.app = app
        self.objects = {}

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] not in ("GET", "DELETE"):
            return await self.app(scope, receive, send)
        match = self.path.fullmatch(scope["path"])
        if not match:
            return await self.app(scope, receive, send)

        oid = match[1]
        access = self.objects.get(oid)
        if access is None:
            access = self.objects[oid] = ObjectAccess()
        access.requests += 1
        try:
            async with access.gate:
                if scope["method"] == "DELETE":
                    await access.idle.wait()
                    return await self.app(scope, receive, send)
                access.readers += 1
                access.idle.clear()
            try:
                # The ASGI call includes transmission of the entire response body.
                return await self.app(scope, receive, send)
            finally:
                access.readers -= 1
                if access.readers == 0:
                    access.idle.set()
        finally:
            access.requests -= 1
            if access.requests == 0:
                del self.objects[oid]
