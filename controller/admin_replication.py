from fastapi import APIRouter
import repair

router = APIRouter()

@router.get("/api/v1/admin/replication")
def replication_status():
    return repair.get_report()