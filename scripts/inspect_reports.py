from __future__ import annotations

from pathlib import Path

import pandas as pd


def inspect_excel(path: Path) -> None:
    print(f"\n=== EXCEL: {path.name} ===")
    xls = pd.ExcelFile(path)
    print("Sheets:", xls.sheet_names)
    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh, engine="openpyxl")
        # Print a compact view: columns + first 3 rows
        cols = [str(c) for c in df.columns]
        print(f"\nSheet: {sh} | shape={df.shape}")
        print("Columns:", cols)
        print(df.head(3).to_string(index=False))


def main() -> None:
    in_dir = Path("in")
    excel_files = sorted(in_dir.glob("*.xlsx")) + sorted(in_dir.glob("*.xls")) + sorted(in_dir.glob("*.XLSX"))
    if not excel_files:
        print("No excel files found in in/ (expected *.xlsx)")
        return
    for p in excel_files:
        inspect_excel(p)


if __name__ == "__main__":
    main()

