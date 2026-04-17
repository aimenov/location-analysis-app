## Employee location MVP

Tool that ingests multiple report exports (Excel/PDF), resolves employee identities across sources, and infers each employee’s current location using configurable priorities and time-aware rules.

### Architecture (high level)

```mermaid
flowchart LR
  A[Input folder in/] --> B[Report discovery]
  B --> C[Parsers per report type]
  C --> D[Normalized events table]
  D --> E[Entity resolution]
  E --> F[Rules engine]
  F --> G[Outputs out/ (CSV/JSON/XLSX)]
  F --> H[Decision Trace sheet (auditable)]
```

### Quickstart

Create a venv, install deps, run the demo pipeline:

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
python launcher.py --asof "2026-04-16T12:00:00Z"
```

Place your Excel/PDF files in `in/` (the app auto-discovers them by content and sheet names) and re-run:

```bash
python launcher.py --asof "2026-04-16T12:00:00Z"
```

Outputs land in `out/`:
- `out/employee_locations.csv`
- `out/employee_locations.json`
- `out/employee_locations.xlsx` (includes `Evidence`, `Source Summary`, and `Decision Trace`)

### Data model (normalized)

All sources are normalized into an events table with (at least):
- **employee_id / email / name**: identifiers (may be partially missing per source)
- **event_type**: `vacation`, `day_off`, `travel`, `office_checkin`, `working_format`
- **start_ts / end_ts**: time interval when the event applies (end is optional)
- **location_raw**: raw location string from the source (if any)
- **source**: source name
- **source_priority**: numeric priority (higher wins)

### Decision policy (summary)

At a given **as-of time** (`--asof`), the engine selects the “best” evidence per employee deterministically:

- **Step 1**: keep events that are active at `asof`
  - `office_checkin` has a decay window (`office_checkin_valid_hours`)
- **Step 2**: rank candidates by:
  - `event_type_priority` (config, lower index wins)
  - `source_priority` (higher wins)
  - most recent `start_ts`
- **Step 3**: map `location_raw` to a canonical label via `config/location_dictionary.csv`

### Where to extend

- Add a new report type:
  - Update detection in `app/report_discovery.py`
  - Add a parser in `app/report_parsers.py`
  - Register it in `PARSERS_BY_TYPE` in `app/pipeline.py`
- Tune entity resolution: `app/entity_resolution.py`
- Tune location mapping: `config/location_dictionary.csv`
- Tune priorities & rules: `config/mvp_config.json`

### Tests

Run:

```bash
python -m pytest -q
```

### Build the .exe (PyInstaller)

```bash
python -m pip install -r requirements.txt
pyinstaller employee_location_app.spec
```

The resulting executable will write logs to `out/employee_location_app.log` and expects an `in/` folder next to the `.exe`.

