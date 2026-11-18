class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message

class ReplicationError(Exception):
    """Could not get enough verified copies of a chunk."""