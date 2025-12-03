from typing import Awaitable, List, Optional, Sequence

class posix:
    @staticmethod
    def rglob(
        path: str,
        glob_pattern: Optional[str] = ...,
        suffixes: Optional[List[str]] = ...,
    ) -> Awaitable[Sequence[str]]: ...
    @staticmethod
    def is_file(path: str) -> Awaitable[bool]: ...
    @staticmethod
    def is_dir(path: str) -> Awaitable[bool]: ...
    @staticmethod
    def iterdir(path: str) -> Awaitable[Sequence[str]]: ...
