# movie-database-analytics

## Importing the TMDB Data

This repository includes a script (`import_data.py`) that loads the TMDB CSV into a local MySQL database.

### Prerequisites
- Python 3.11+ (installed)
- `mysql-connector-python` (installed via pip)
- A running MySQL server with a database named `TMDBMovie` (or update `DB` in `import_data.py`)

### Running the import
From this project folder run:

```bash
python import_data.py --chunksize 50000
```

This:
- reads `TMDB_movie_dataset_v11.csv` in chunks
- imports rows into MySQL
- generates synthetic language codes (e.g., `x0001`, `x0002`) for unknown languages

### Optional flags
- `--chunksize <n>`: number of rows processed per chunk (default: 10000)
- `--max-rows <n>`: stop after importing this many rows (useful for testing)
