from __future__ import annotations

import re
import sys
import threading
import tkinter as tk
from datetime import datetime, timezone
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

import pandas as pd

from app.logging_setup import LoggingConfig, configure_logging
from app.pipeline import parse_asof, presentable_locations_df, run_pipeline


def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _resource_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    return _base_dir()


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}(:\d{2})?$")


ALL_COLUMNS = [
    "Employee Name",
    "Location",
    "Recorded at",
    "Based on",
    "Employee ID",
    "Report file",
]


class ViewerApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Employee locations — viewer")
        self.geometry("1100x720")
        self.minsize(800, 500)

        base = _base_dir()
        res = _resource_dir()

        self._input_dir = tk.StringVar(value=str(base / "in"))
        self._output_dir = tk.StringVar(value=str(base / "out"))
        self._config_path = str(res / "config" / "mvp_config.json")

        utc_now = datetime.now(timezone.utc).replace(microsecond=0)
        self._date_var = tk.StringVar(value=utc_now.strftime("%Y-%m-%d"))
        self._time_var = tk.StringVar(value=utc_now.strftime("%H:%M"))

        self._filter_column = tk.StringVar(value="All columns")
        self._filter_text = tk.StringVar()

        self._full_df: pd.DataFrame | None = None
        self._last_asof_str = ""
        self._busy = False

        default_hidden = {"Employee ID", "Report file"}
        self._column_visible: dict[str, tk.BooleanVar] = {
            c: tk.BooleanVar(value=(c not in default_hidden)) for c in ALL_COLUMNS
        }

        log_path = base / "out" / "viewer_app.log"
        configure_logging(cfg=LoggingConfig(level="INFO", log_file=str(log_path), console=False))

        self._build_ui()

    def _build_ui(self) -> None:
        style = ttk.Style(self)
        # Try to use a modern built-in theme where available.
        try:
            style.theme_use("vista")
        except Exception:
            try:
                style.theme_use("clam")
            except Exception:
                pass

        default_font = ("Segoe UI", 10)
        style.configure(".", font=default_font)

        top = ttk.LabelFrame(self, text="Paths", padding=8)
        top.pack(fill=tk.X, padx=8, pady=6)

        self._row_path(top, 0, "Input folder (reports):", self._input_dir, self._browse_input)
        self._row_path(top, 1, "Output folder:", self._output_dir, self._browse_output)

        query = ttk.LabelFrame(self, text="As-of date & time", padding=8)
        query.pack(fill=tk.X, padx=8, pady=6)

        ttk.Label(query, text="Date (YYYY-MM-DD):").grid(row=0, column=0, sticky=tk.W, padx=(0, 8))
        ttk.Entry(query, textvariable=self._date_var, width=14).grid(row=0, column=1, sticky=tk.W)

        ttk.Label(query, text="Time (HH:MM):").grid(row=0, column=2, sticky=tk.W, padx=(16, 8))
        ttk.Entry(query, textvariable=self._time_var, width=10).grid(row=0, column=3, sticky=tk.W)

        self._run_btn = ttk.Button(query, text="Compute locations…", command=self._on_run)
        self._run_btn.grid(row=0, column=4, padx=(24, 0))

        dash = ttk.LabelFrame(self, text="Dashboard — employees per location", padding=8)
        dash.pack(fill=tk.X, padx=8, pady=6)

        self._dash_tree = ttk.Treeview(dash, columns=("location", "count"), show="headings", height=5)
        self._dash_tree.heading("location", text="Location")
        self._dash_tree.heading("count", text="Count")
        self._dash_tree.column("location", width=420)
        self._dash_tree.column("count", width=80)
        dd_scroll = ttk.Scrollbar(dash, orient=tk.VERTICAL, command=self._dash_tree.yview)
        self._dash_tree.configure(yscrollcommand=dd_scroll.set)
        self._dash_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        dd_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        filt = ttk.LabelFrame(self, text="Filter rows", padding=8)
        filt.pack(fill=tk.X, padx=8, pady=6)

        ttk.Label(filt, text="Column:").grid(row=0, column=0, sticky=tk.W)
        self._column_combo = ttk.Combobox(
            filt,
            textvariable=self._filter_column,
            values=["All columns"] + ALL_COLUMNS,
            width=26,
            state="readonly",
        )
        self._column_combo.grid(row=0, column=1, sticky=tk.W, padx=(6, 16))

        ttk.Label(filt, text="Contains:").grid(row=0, column=2, sticky=tk.W)
        ent = ttk.Entry(filt, textvariable=self._filter_text, width=40)
        ent.grid(row=0, column=3, sticky=tk.W)
        ent.bind("<Return>", lambda _: self._apply_filter())

        ttk.Button(filt, text="Apply filter", command=self._apply_filter).grid(row=0, column=4, padx=(12, 0))
        ttk.Button(filt, text="Clear", command=self._clear_filter).grid(row=0, column=5, padx=(8, 0))
        ttk.Button(filt, text="Export to Excel…", command=self._export_excel).grid(row=0, column=6, padx=(16, 0))

        cols_frm = ttk.LabelFrame(self, text="Visible columns", padding=8)
        cols_frm.pack(fill=tk.X, padx=8, pady=6)

        for i, name in enumerate(ALL_COLUMNS):
            chk = ttk.Checkbutton(cols_frm, text=name, variable=self._column_visible[name], command=self._refresh_columns)
            chk.grid(row=i // 4, column=i % 4, sticky=tk.W, padx=(0, 16), pady=2)

        tbl_wrap = ttk.Frame(self)
        tbl_wrap.pack(fill=tk.BOTH, expand=True, padx=8, pady=(6, 8))

        vsb = ttk.Scrollbar(tbl_wrap, orient=tk.VERTICAL)
        hsb = ttk.Scrollbar(tbl_wrap, orient=tk.HORIZONTAL)
        self._tree = ttk.Treeview(tbl_wrap, show="headings", height=14, yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.config(command=self._tree.yview)
        hsb.config(command=self._tree.xview)

        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tbl_wrap.rowconfigure(0, weight=1)
        tbl_wrap.columnconfigure(0, weight=1)

        self._status = ttk.Label(self, text="Load reports into Input folder, set date/time, then click Compute.", anchor=tk.W)
        self._status.pack(fill=tk.X, padx=8, pady=(0, 8))

        # Table styling
        style.configure("Treeview", rowheight=26)
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
        self._tree.tag_configure("odd", background="#FAFAFA")
        self._tree.tag_configure("even", background="#FFFFFF")

    def _row_path(self, parent: ttk.Frame, row: int, label: str, var: tk.StringVar, browse_cmd) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
        ttk.Entry(parent, textvariable=var, width=72).grid(row=row, column=1, sticky=tk.EW, padx=8, pady=2)
        ttk.Button(parent, text="Browse…", command=browse_cmd).grid(row=row, column=2, pady=2)
        parent.columnconfigure(1, weight=1)

    def _browse_input(self) -> None:
        p = filedialog.askdirectory(initialdir=self._input_dir.get() or ".")
        if p:
            self._input_dir.set(p)

    def _browse_output(self) -> None:
        p = filedialog.askdirectory(initialdir=self._output_dir.get() or ".")
        if p:
            self._output_dir.set(p)

    def _combine_asof_string(self) -> str:
        d = self._date_var.get().strip()
        t = self._time_var.get().strip()
        if not DATE_RE.match(d):
            raise ValueError(f"Invalid date '{d}'. Use YYYY-MM-DD.")
        if not TIME_RE.match(t):
            raise ValueError(f"Invalid time '{t}'. Use HH:MM or HH:MM:SS.")
        if len(t) == 5:
            t = f"{t}:00"
        return f"{d}T{t}Z"

    def _on_run(self) -> None:
        if self._busy:
            return
        try:
            asof_str = self._combine_asof_string()
            parse_asof(asof_str)
        except ValueError as e:
            messagebox.showerror("Invalid date/time", str(e))
            return

        self._busy = True
        self._run_btn.configure(state="disabled")
        self._status.configure(text="Running pipeline…")

        inp = self._input_dir.get().strip()
        out = self._output_dir.get().strip()

        def work() -> None:
            exc: BaseException | None = None
            pdf: pd.DataFrame | None = None
            rows = 0
            try:
                _, loc_df = run_pipeline(
                    config_path=self._config_path,
                    asof=parse_asof(asof_str),
                    input_dir=inp,
                    output_dir=out,
                )
                pdf = presentable_locations_df(loc_df)
                rows = len(pdf.index)
            except BaseException as e:
                exc = e

            def finish() -> None:
                self._busy = False
                self._run_btn.configure(state="normal")
                if exc is not None:
                    messagebox.showerror("Pipeline failed", str(exc))
                    self._status.configure(text="Pipeline failed.")
                    return
                assert pdf is not None
                self._full_df = pdf
                self._last_asof_str = asof_str
                self._apply_filter()

            self.after(0, finish)

        threading.Thread(target=work, daemon=True).start()

    def _filtered_df(self) -> pd.DataFrame | None:
        if self._full_df is None:
            return None
        df = self._full_df.copy()
        col = self._filter_column.get()
        needle = self._filter_text.get().strip().lower()
        if needle:
            if col == "All columns":
                def row_match(row: pd.Series) -> bool:
                    blob = " ".join("" if pd.isna(v) else str(v) for v in row).lower()
                    return needle in blob

                mask = df.apply(row_match, axis=1)
            else:
                if col not in df.columns:
                    return df
                mask = df[col].astype(str).str.contains(needle, case=False, na=False, regex=False)
            df = df.loc[mask]
        return df

    def _apply_filter(self) -> None:
        self._refresh_columns()
        df = self._filtered_df()
        n = 0 if df is None else len(df.index)
        total = 0 if self._full_df is None else len(self._full_df.index)
        prefix = f"As-of {self._last_asof_str}: " if self._last_asof_str else ""
        if self._full_df is not None:
            self._status.configure(text=f"{prefix}showing {n} of {total} row(s).")

    def _clear_filter(self) -> None:
        self._filter_text.set("")
        self._filter_column.set("All columns")
        self._refresh_columns()
        if self._full_df is not None:
            total = len(self._full_df.index)
            prefix = f"As-of {self._last_asof_str}: " if self._last_asof_str else ""
            self._status.configure(text=f"{prefix}showing all {total} row(s).")

    def _refresh_columns(self) -> None:
        self._refresh_dashboard()
        self._refresh_table()

    def _refresh_dashboard(self) -> None:
        for item in self._dash_tree.get_children():
            self._dash_tree.delete(item)
        df = self._filtered_df()
        if df is None or df.empty:
            return
        if "Location" not in df.columns:
            return
        counts = df.groupby("Location", dropna=False).size().reset_index(name="Count")
        counts = counts.sort_values("Count", ascending=False)
        for _, r in counts.iterrows():
            loc = "" if pd.isna(r["Location"]) else str(r["Location"])
            self._dash_tree.insert("", tk.END, values=(loc, int(r["Count"])))

    def _refresh_table(self) -> None:
        for item in self._tree.get_children():
            self._tree.delete(item)

        df = self._filtered_df()
        visible = [c for c in ALL_COLUMNS if self._column_visible[c].get()]
        if not visible:
            self._tree.configure(columns=())
            return

        self._tree.configure(columns=visible)
        for c in visible:
            self._tree.heading(c, text=c)
            anchor = tk.W
            width = 160
            if c in {"Recorded at"}:
                width = 170
            if c in {"Employee ID"}:
                width = 110
            if c in {"Based on"}:
                width = 120
            if c in {"Report file"}:
                width = 160
            self._tree.column(c, width=width, stretch=True, anchor=anchor)

        if df is None or df.empty:
            return

        for idx, (_, row) in enumerate(df.iterrows()):
            vals: list[Any] = []
            for c in visible:
                v = row.get(c)
                if isinstance(v, pd.Timestamp):
                    vals.append(v.strftime("%Y-%m-%d %H:%M") if pd.notna(v) else "")
                elif pd.isna(v):
                    vals.append("")
                else:
                    vals.append(str(v))
            tag = "even" if idx % 2 == 0 else "odd"
            self._tree.insert("", tk.END, values=tuple(vals), tags=(tag,))

    def _export_excel(self) -> None:
        df = self._filtered_df()
        if df is None or df.empty:
            messagebox.showinfo("Export", "Nothing to export (no rows).")
            return

        visible = [c for c in ALL_COLUMNS if self._column_visible[c].get()]
        if not visible:
            messagebox.showinfo("Export", "No columns selected to export.")
            return

        asof_tag = (self._last_asof_str or "asof").replace(":", "").replace("Z", "")
        default_name = f"employee_locations_view_{asof_tag}.xlsx"
        initial_dir = self._output_dir.get().strip() or str(_base_dir() / "out")
        out_path = filedialog.asksaveasfilename(
            title="Export to Excel",
            defaultextension=".xlsx",
            initialdir=initial_dir,
            initialfile=default_name,
            filetypes=[("Excel", "*.xlsx")],
        )
        if not out_path:
            return

        try:
            df_out = df[visible].copy()
            # Excel cannot store timezone-aware datetimes.
            for col in df_out.columns:
                if pd.api.types.is_datetime64tz_dtype(df_out[col]):
                    df_out[col] = df_out[col].dt.tz_localize(None)
                elif pd.api.types.is_object_dtype(df_out[col]):
                    df_out[col] = df_out[col].apply(
                        lambda v: v.replace(tzinfo=None) if hasattr(v, "tzinfo") and getattr(v, "tzinfo", None) else v
                    )

            # Ensure "Recorded at" is a real Excel datetime column (not strings).
            if "Recorded at" in df_out.columns:
                s = df_out["Recorded at"]
                if pd.api.types.is_datetime64tz_dtype(s):
                    df_out["Recorded at"] = s.dt.tz_localize(None)
                elif not pd.api.types.is_datetime64_any_dtype(s):
                    df_out["Recorded at"] = pd.to_datetime(s, utc=True, errors="coerce").dt.tz_localize(None)
                else:
                    # datetime64[ns] naive is fine
                    pass

            df_out.to_excel(out_path, index=False)
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))
            return

        messagebox.showinfo("Export", f"Wrote {len(df_out.index)} row(s) to:\n{out_path}")


def main() -> int:
    app = ViewerApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
