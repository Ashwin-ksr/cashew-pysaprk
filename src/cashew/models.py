from dataclasses import dataclass, field

@dataclass
class OperationResult:
    success: bool
    rows_processed: int = 0
    message: str = ""
    error: BaseException | None = None

    def raise_if_failed(self) -> OperationResult:
        """Raise the stored exception if the operation failed; otherwise return self."""
        if not self.success:
            if self.error is not None:
                raise self.error
            raise RuntimeError(self.message or "Operation failed")
        return self

@dataclass
class SQLiteLoadResult(OperationResult):
    tables_processed: int = 0
    failed_tables: dict[str, str] = field(default_factory=dict)