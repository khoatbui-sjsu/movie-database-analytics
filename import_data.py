import argparse
import pandas as pd
import mysql.connector

DB = {
    "host": "localhost",
    "user": "root",
    "password": "25252525",
    "database": "TMDBMovie"
}

# CSV config
# CSV_MOVIE = "Small_data_movies.csv"
CSV_MOVIE = "TMDB_movie_dataset_v11.csv"
CSV_ISO = "language_iso.csv"

# Synthetic language code generator (keeps codes <= 5 chars)
_SYNTHETIC_MAX = 9999  # x0001 .. x9999 (fits VARCHAR(5))


def connect():
    return mysql.connector.connect(**DB)


def cursor(conn):
    return conn.cursor(buffered=True)


def _format_synthetic_code(counter: int) -> str:
    """Format the synthetic code as x0001, x0002, ..."""
    return f"x{counter:04d}"


def load_language_cache(cur):
    """Load language caches from the database."""
    cur.execute("SELECT language_code, language_name FROM language")
    valid_languages = set()
    code_to_name = {}
    synthetic_counter = 0

    for code, name in cur.fetchall():
        valid_languages.add(code)
        code_to_name[code] = name

        if code.startswith("x") and len(code) == 5 and code[1:].isdigit():
            synthetic_counter = max(synthetic_counter, int(code[1:]))

    name_to_iso = {name: code for code, name in code_to_name.items()}
    return {
        "valid_languages": valid_languages,
        "code_to_name": code_to_name,
        "name_to_iso": name_to_iso,
        "synthetic_counter": synthetic_counter,
    }


def create_synthetic_language(cur, caches, name: str) -> str:
    """Create a synthetic language code for a name and insert it into the DB."""
    valid_languages = caches["valid_languages"]
    code_to_name = caches["code_to_name"]

    # Keep counter in sync with the database in case other workers inserted new codes.
    cur.execute(
        "SELECT MAX(CAST(SUBSTRING(language_code, 2) AS UNSIGNED)) FROM language WHERE language_code LIKE 'x____'"
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        counter = max(caches["synthetic_counter"], int(row[0]))
    else:
        counter = caches["synthetic_counter"]

    counter += 1
    code = _format_synthetic_code(counter)

    # Ensure we don’t collide with an existing code for a different name.
    while code in valid_languages and code_to_name.get(code) != name:
        counter += 1
        if counter > _SYNTHETIC_MAX:
            raise RuntimeError("Ran out of synthetic language codes")
        code = _format_synthetic_code(counter)

    caches["synthetic_counter"] = counter

    if code not in valid_languages:
        cur.execute(
            "INSERT INTO language(language_code, language_name) VALUES (%s, %s)",
            (code, name),
        )
        valid_languages.add(code)
        code_to_name[code] = name
        caches["name_to_iso"][name] = code

    return code


def ensure_language(cur, caches, value: str | None) -> str | None:
    """Ensure a language code exists for a value (code or name)."""
    if value is None:
        return None

    value = str(value).strip()
    if value == "":
        return None

    valid_languages = caches["valid_languages"]
    name_to_iso = caches["name_to_iso"]

    if value in valid_languages:
        return value

    if value in name_to_iso:
        return name_to_iso[value]

    if len(value) <= 5 and value.isalnum():
        return create_synthetic_language(cur, caches, value)

    return create_synthetic_language(cur, caches, value)


def ensure_lookup(cur, name, table, cache, col_name):
    if name in cache:
        return cache[name]

    cur.execute(f"INSERT IGNORE INTO {table}({col_name}) VALUES (%s)", (name,))
    cur.execute(f"SELECT {table}_id FROM {table} WHERE {col_name}=%s", (name,))
    row = cur.fetchone()
    if row:
        cache[name] = row[0]
        return row[0]
    return None


def insert_movie(cur, caches, row):
    lang = row.get("original_language")
    if lang is not None and str(lang).strip() != "":
        lang = ensure_language(cur, caches, lang)
    else:
        lang = None

    sql = """
        INSERT IGNORE INTO movie(
            movie_id, title, vote_average, vote_count, status, release_date, revenue, runtime,
            adult, backdrop_path, budget, homepage, imdb_id, original_language_code,
            original_title, overview, popularity, poster_path, tagline
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    data = (
        int(row["id"]),
        row.get("title"),
        float(row["vote_average"]) if row.get("vote_average") is not None else None,
        int(row["vote_count"]) if row.get("vote_count") is not None else None,
        row.get("status"),
        row.get("release_date"),
        float(row["revenue"]) if row.get("revenue") is not None else None,
        int(row["runtime"]) if row.get("runtime") is not None else None,
        bool(row["adult"]) if row.get("adult") is not None else None,
        row.get("backdrop_path"),
        int(row["budget"]) if row.get("budget") is not None else None,
        row.get("homepage"),
        row.get("imdb_id"),
        lang,
        row.get("original_title"),
        row.get("overview"),
        float(row["popularity"]) if row.get("popularity") is not None else None,
        row.get("poster_path"),
        row.get("tagline"),
    )

    cur.execute(sql, data)


def insert_genres(cur, caches, movie_id, text):
    if text is None or str(text).strip() == "":
        return
    for g in str(text).split(","):
        g = g.strip()
        if g:
            gid = ensure_lookup(cur, g, "genre", caches["genre"], "genre_name")
            cur.execute("INSERT IGNORE INTO movie_genre(movie_id, genre_id) VALUES (%s,%s)", (movie_id, gid))


def insert_companies(cur, caches, movie_id, text):
    if text is None or str(text).strip() == "":
        return
    for c in str(text).split(","):
        c = c.strip()
        if c:
            cid = ensure_lookup(cur, c, "company", caches["company"], "company_name")
            cur.execute("INSERT IGNORE INTO movie_company(movie_id, company_id) VALUES (%s,%s)", (movie_id, cid))


def insert_countries(cur, caches, movie_id, text):
    if text is None or str(text).strip() == "":
        return
    for c in str(text).split(","):
        c = c.strip()
        if c:
            cid = ensure_lookup(cur, c, "country", caches["country"], "country_name")
            cur.execute("INSERT IGNORE INTO movie_country(movie_id, country_id) VALUES (%s,%s)", (movie_id, cid))


def insert_keywords(cur, caches, movie_id, text):
    if text is None or str(text).strip() == "":
        return
    for k in str(text).split(","):
        k = k.strip()
        if k:
            kid = ensure_lookup(cur, k, "keyword", caches["keyword"], "keyword_name")
            cur.execute("INSERT IGNORE INTO movie_keyword(movie_id, keyword_id) VALUES (%s,%s)", (movie_id, kid))


def insert_spoken(cur, caches, movie_id, text):
    if text is None or str(text).strip() == "":
        return
    for item in str(text).split(","):
        item = item.strip()
        if item:
            code = ensure_language(cur, caches, item)
            cur.execute(
                "INSERT IGNORE INTO movie_language(movie_id, language_code) VALUES (%s,%s)",
                (movie_id, code),
            )


def process_chunk(df_chunk, chunk_index, log_prefix=""):
    """Process one CSV chunk in its own database connection."""
    conn = connect()
    cur = cursor(conn)

    caches = load_language_cache(cur)
    caches.update({
        "genre": {},
        "company": {},
        "country": {},
        "keyword": {},
    })

    for _, row in df_chunk.iterrows():
        movie_id = int(row["id"])
        insert_movie(cur, caches, row)
        insert_genres(cur, caches, movie_id, row.get("genres"))
        insert_companies(cur, caches, movie_id, row.get("production_companies"))
        insert_countries(cur, caches, movie_id, row.get("production_countries"))
        insert_keywords(cur, caches, movie_id, row.get("keywords"))
        insert_spoken(cur, caches, movie_id, row.get("spoken_languages"))

    conn.commit()
    cur.close()
    conn.close()

    print(f"{log_prefix}Chunk {chunk_index} done (rows: {len(df_chunk)})")
    return len(df_chunk)


def _count_csv_rows(path: str) -> int:
    """Count the number of data rows in a CSV (excludes header)."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return max(0, sum(1 for _ in f) - 1)
    except FileNotFoundError:
        return 0


def import_csv(chunksize=10000, max_rows=None):
    # Ensure the ISO language table is populated before processing.
    conn = connect()
    cur = cursor(conn)
    print("Importing ISO languages (once)...")
    import_iso_languages(cur)
    conn.commit()
    cur.close()
    conn.close()

    total_rows = _count_csv_rows(CSV_MOVIE)
    if total_rows == 0:
        print("No rows found in CSV or file is missing.")
        return

    print(f"Reading CSV in chunks of {chunksize} rows (total approx {total_rows})...")

    submitted_rows = 0
    completed_rows = 0

    for chunk_index, df in enumerate(pd.read_csv(CSV_MOVIE, chunksize=chunksize)):
        # 1. Convert dates (invalid dates become NaT)
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date
        
        df = df.astype(object)
        # 2. Convert all pandas missing values (NaN, NaT) to standard Python None
        df = df.where(pd.notna(df), None)

        if max_rows is not None and submitted_rows >= max_rows:
            break

        if max_rows is not None and submitted_rows + len(df) > max_rows:
            df = df.iloc[: max_rows - submitted_rows]

        submitted_rows += len(df)

        completed_rows += process_chunk(df, chunk_index)
        percent = (completed_rows / total_rows) * 100 if total_rows else 0
        print(f"Progress: {completed_rows}/{total_rows} ({percent:.1f}%)")

        if max_rows is not None and submitted_rows >= max_rows:
            break

    print(f"Import complete (total rows processed: {completed_rows})")


def import_iso_languages(cur):
    df = pd.read_csv(CSV_ISO)
    for _, row in df.iterrows():
        code = str(row["code"]).strip()
        name = str(row["name"]).strip()

        cur.execute(
            "INSERT IGNORE INTO language(language_code, language_name) VALUES (%s, %s)",
            (code, name),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import TMDB data into MySQL.")
    parser.add_argument("--chunksize", type=int, default=10000, help="Number of rows per chunk")
    parser.add_argument("--max-rows", type=int, default=None, help="Stop after processing this many rows")
    args = parser.parse_args()

    import_csv(chunksize=args.chunksize, max_rows=args.max_rows)