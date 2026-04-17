from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class TableValidationError(ValueError):
    table: str
    problems: list[str]

    def __str__(self) -> str:
        details = "\n".join(f"- {p}" for p in self.problems)
        return f"Invalid {self.table} table:\n{details}"


def require_columns(*, df: pd.DataFrame, table: str, required: Iterable[str]) -> None:
    req = list(required)
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise TableValidationError(
            table=table,
            problems=[f"Missing required columns: {missing}", f"Available columns: {list(df.columns)}"],
        )

