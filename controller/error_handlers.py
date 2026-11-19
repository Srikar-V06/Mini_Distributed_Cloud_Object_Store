import contextvars, logging, time, uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.exceptions import HTTPException as StarletteHTTPException

from errors import ApiError
from event_log import JsonFormatter, event

log = logging.getLogger("http")
request_id_var = contextvars.ContextVar("request_id", default="-")
QUIET_PATHS = ("/internal/workers/heartbeat", "/health")   # logged at DEBUG, not INFO

STATUS_CODES = {400: "bad_request", 404: "not_found", 405: "method_not_allowed",
                409: "conflict", 413: "payload_too_large", 503: "unavailable"}
DEFAULT_MESSAGES = {404: "resource not found", 405: "method not allowed"}


def error_body(code: str, message: str, **extra) -> dict:
    return {"error": code, "message": message, **extra}


# ---------- logging with request IDs ----------
class RequestIdFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "request_id"):
            record.request_id = request_id_var.get()
        return True


def setup_logging():
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=logging.INFO)
    fmt = JsonFormatter()
    for h in root.handlers:
        h.addFilter(RequestIdFilter())
        h.setFormatter(fmt)


# ---------- request ID + access log (pure ASGI, stream-safe) ----------
class RequestIdMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        rid = uuid.uuid4().hex[:12]
        scope["request_id"] = rid                   # same dict object is seen by the 500 handler
        token = request_id_var.set(rid)
        started, status, failed = time.monotonic(), 0, False

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                MutableHeaders(scope=message)["X-Request-ID"] = rid
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            failed = True
            raise                                   # never swallow: aborted streams must stay aborted
        finally:
            ms = (time.monotonic() - started) * 1000
            level = logging.DEBUG if scope["path"] in QUIET_PATHS else logging.INFO
            log.log(level, "%s %s -> %s %.0fms%s", scope["method"], scope["path"], status, ms,
                    " (exception)" if failed else "",
                    extra=event("http_request", method=scope["method"], path=scope["path"],
                                status=status, duration_ms=round(ms), failed=failed))
            request_id_var.reset(token)


# ---------- handlers ----------
def install_error_handling(app: FastAPI):
    setup_logging()
    app.add_middleware(RequestIdMiddleware)

    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError):
        return JSONResponse(status_code=exc.status, content=error_body(exc.code, exc.message))

    @app.exception_handler(RequestValidationError)          # FastAPI's 422 -> our 400
    async def validation_handler(request: Request, exc: RequestValidationError):
        errs = exc.errors()
        if errs:
            where = ".".join(str(p) for p in errs[0].get("loc", ()))
            msg = f"invalid request ({where}): {errs[0].get('msg', 'invalid value')}"
        else:
            msg = "invalid request"
        return JSONResponse(status_code=400, content=error_body("invalid_request", msg))

    @app.exception_handler(StarletteHTTPException)          # unknown route, wrong method, parse errors
    async def http_handler(request: Request, exc: StarletteHTTPException):
        code = STATUS_CODES.get(exc.status_code, "http_error")
        msg = DEFAULT_MESSAGES.get(exc.status_code) or str(exc.detail)
        return JSONResponse(status_code=exc.status_code, content=error_body(code, msg),
                            headers=getattr(exc, "headers", None))   # keeps the Allow header on 405

    @app.exception_handler(Exception)                       # catch-all: hide the trace, log it
    async def unhandled_handler(request: Request, exc: Exception):
        rid = request.scope.get("request_id", "-")
        log.error("unhandled exception on %s %s", request.method, request.url.path,
                  exc_info=exc, extra={"request_id": rid})
        return JSONResponse(status_code=500,
                            content=error_body("internal_error", "internal server error", request_id=rid),
                            headers={"X-Request-ID": rid})
