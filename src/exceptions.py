class DataFetchError(RuntimeError):
    def __init__(self, provider: str, message: str):  # noqa: D401
        super().__init__(f"[{provider}] {message}")
        self.provider = provider
        self.message = message

__all__ = ["DataFetchError"]
