from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/_debug")

@router.get("/int/{n}")
def int_echo(n: int):          # n='abc' -> FastAPI validation error (422)
    return {"n": n}

@router.get("/boom")
def boom():
    raise RuntimeError("deliberate failure for testing")