# dsci551-project

Live app: https://kunfupen-dsci551-project-app-hkgwt3.streamlit.app/#books-explorer

Books Explorer: a Streamlit app over a Postgres database of books, plus helper scripts to clean and load the dataset.

## Prerequisites
- Python 3.10+ (3.11 recommended)
- PostgreSQL 13+
- `psql` client available on your PATH

## Environment Setup
macOS/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Windows (PowerShell):
```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Configure the Project
Create a `.env` file in the project root:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=books
DB_USER=postgres
DB_PASSWORD=your_password
DB_SSLMODE=disable
```

Notes:
- `DB_HOST` can be a hostname or full URL; the app strips `http://` or `https://` if present.
- Use `DB_SSLMODE=require` for hosted databases that need TLS.

## Data Preparation
Clean the raw CSV into a consistent format:

```bash
python load_data.py
```

This reads `data/books.csv` and writes `data/books_clean.csv`.

## Load Data into Postgres
Create the database if you do not already have it:

```bash
createdb books
```

Then load schema and data:

```bash
psql -d books -f schema.sql
```

If you see `\copy: parse error at end of line`, `schema.sql` has a multi-line `\copy` command.
`psql` requires `\copy` to be a single line. Update `schema.sql` accordingly, then re-run.

The script loads `data/books_clean.csv` by default. To load a different file:

```bash
psql -d books -v csv_path='data/your_file.csv' -f schema.sql
```

## Run the Application
```bash
streamlit run app.py
```

Open the URL printed by Streamlit (usually `http://localhost:8501`).

## Reproduce Results
From a clean checkout:

1. Set up the environment and install dependencies.
2. Generate the cleaned dataset with `python load_data.py`.
3. Load the data using `psql -d books -f schema.sql`.
4. Run the app with `streamlit run app.py`.
5. In the UI, use:
	- **Explain + Index** tab to compare plans before/after indexes.
	- **Selectivity Demo** tab to see planner decisions across thresholds.
