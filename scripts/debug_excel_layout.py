from __future__ import annotations

from pathlib import Path

import pandas as pd


def find_header_row(path: Path, sheet_name: str, needle_substrings: list[str], max_scan: int = 50) -> int | None:
    df = pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")
    for i in range(min(max_scan, len(df.index))):
        row_vals = df.iloc[i].astype(str).fillna("")
        hay = " ".join(row_vals.tolist()).lower()
        if all(ns.lower() in hay for ns in needle_substrings):
            return i
    return None


def show_after_header(path: Path, sheet_name: str, header_row: int, rows: int = 5) -> None:
    df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, engine="openpyxl")
    print(f"\nSheet={sheet_name} header_row={header_row} shape={df.shape}")
    print("Columns:", [str(c) for c in df.columns])
    print(df.head(rows).to_string(index=False))


def main() -> None:
    root = Path("in")
    hr = root / "05 - HR Attendance Detail Report.xlsx"
    absd = root / "AbsenceDetails-Component.xlsx"

    print("== HR Attendance ==")
    for sh in ["Detail By Cardholders", "Detail Log By Cardholders"]:
        # We expect the header to contain either "NK First Name - Last name" and "Reader"
        header_row = find_header_row(hr, sh, ["NK", "Reader"])
        if header_row is None:
            header_row = find_header_row(hr, sh, ["Badge #", "Date", "Time"])
        print(f"Found header_row for {sh}: {header_row}")
        if header_row is not None:
            show_after_header(hr, sh, header_row)

    print("\n== AbsenceDetails ==")
    sheet = "Excel Output"
    header_row = find_header_row(absd, sheet, ["Last Name (in English)", "First Name (in English)"])
    print(f"Found header_row: {header_row}")
    if header_row is not None:
        show_after_header(absd, sheet, header_row, rows=7)


if __name__ == "__main__":
    main()

