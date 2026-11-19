class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message

class ReplicationError(Exception):
    """Could not get enough verified copies of a chunk."""
class BadRequest(ApiError):
    def __init__(self, message: str, code: str = "bad_request"):
        super().__init__(400, code, message)

class NotFound(ApiError):
    def __init__(self, message: str = "object not found", code: str = "not_found"):
        super().__init__(404, code, message)

class Conflict(ApiError):
    def __init__(self, message: str, code: str = "conflict"):
        super().__init__(409, code, message)

class Unavailable(ApiError):
    def __init__(self, message: str, code: str = "unavailable"):
        super().__init__(503, code, message)