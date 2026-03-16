import pandas as pd
import mysql.connector

DB = {
    "host": "localhost",
    "user": "root",
    "password": "101188",
    "database": "TMDBMovie"
}

CSV_MOVIE = "Small_data_movies.csv"
CSV_ISO = "language_iso.csv"

def connect():
    return mysql.connector.connect(**DB)

def cursor(conn):
    return conn.cursor(buffered=True)

cache_lang = {}
cache_genre = {}
cache_company = {}
cache_country = {}
cache_keyword = {}

name_to_iso = {}
valid_languages = set()
synthetic_counter = 0


# ==========================
# IMPORT ISO CSV
# ==========================
def import_iso_languages(cur):
    df = pd.read_csv(CSV_ISO)
    for _, row in df.iterrows():
        code = str(row["code"]).strip()
        name = str(row["name"]).strip()

        cur.execute(
            "INSERT IGNORE INTO language(language_code, language_name) VALUES (%s, %s)",
            (code, name)
        )
        name_to_iso[name] = code


def load_valid_languages(cur):
    global valid_languages
    cur.execute("SELECT language_code FROM language")
    valid_languages = {row[0] for row in cur.fetchall()}


def load_synthetic_counter(cur):
    global synthetic_counter
    cur.execute("SELECT language_code FROM language WHERE language_code LIKE 'x%'")
    nums = []
    for (code,) in cur.fetchall():
        if code[1:].isdigit():
            nums.append(int(code[1:]))
    synthetic_counter = max(nums) if nums else 0


def create_synthetic_language(cur, name):
    global synthetic_counter
    synthetic_counter += 1
    code = f"x{synthetic_counter}"

    cur.execute(
        "INSERT INTO language(language_code, language_name) VALUES (%s, %s)",
        (code, name)
    )

    valid_languages.add(code)
    cache_lang[code] = code
    return code


def ensure_language(cur, value):
    value = value.strip()

    if value in valid_languages:
        return value

    if value in name_to_iso:
        return name_to_iso[value]

    return create_synthetic_language(cur, value)


# ==========================
# GENERIC LOOKUP
# ==========================
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


# ==========================
# INSERT MOVIE
# ==========================
def insert_movie(cur, row):
    lang = row["original_language"]
    if pd.notna(lang) and lang != "":
        lang = ensure_language(cur, lang)
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
        row["title"],
        float(row["vote_average"]) if not pd.isna(row["vote_average"]) else None,
        int(row["vote_count"]) if not pd.isna(row["vote_count"]) else None,
        row["status"],
        row["release_date"] if not pd.isna(row["release_date"]) else None,
        float(row["revenue"]) if not pd.isna(row["revenue"]) else None,
        int(row["runtime"]) if not pd.isna(row["runtime"]) else None,
        bool(row["adult"]) if not pd.isna(row["adult"]) else None,
        row["backdrop_path"],
        int(row["budget"]) if not pd.isna(row["budget"]) else None,
        row["homepage"],
        row["imdb_id"],
        lang,
        row["original_title"],
        row["overview"],
        float(row["popularity"]) if not pd.isna(row["popularity"]) else None,
        row["poster_path"],
        row["tagline"]
    )

    cur.execute(sql, data)


# ==========================
# INSERT LINK TABLES
# ==========================
def insert_genres(cur, movie_id, text):
    if pd.isna(text) or text == "":
        return
    for g in str(text).split(","):
        g = g.strip()
        if g:
            gid = ensure_lookup(cur, g, "genre", cache_genre, "genre_name")
            cur.execute("INSERT IGNORE INTO movie_genre(movie_id, genre_id) VALUES (%s,%s)", (movie_id, gid))


def insert_companies(cur, movie_id, text):
    if pd.isna(text) or text == "":
        return
    for c in str(text).split(","):
        c = c.strip()
        if c:
            cid = ensure_lookup(cur, c, "company", cache_company, "company_name")
            cur.execute("INSERT IGNORE INTO movie_company(movie_id, company_id) VALUES (%s,%s)", (movie_id, cid))


def insert_countries(cur, movie_id, text):
    if pd.isna(text) or text == "":
        return
    for c in str(text).split(","):
        c = c.strip()
        if c:
            cid = ensure_lookup(cur, c, "country", cache_country, "country_name")
            cur.execute("INSERT IGNORE INTO movie_country(movie_id, country_id) VALUES (%s,%s)", (movie_id, cid))


def insert_keywords(cur, movie_id, text):
    if pd.isna(text) or text == "":
        return
    for k in str(text).split(","):
        k = k.strip()
        if k:
            kid = ensure_lookup(cur, k, "keyword", cache_keyword, "keyword_name")
            cur.execute("INSERT IGNORE INTO movie_keyword(movie_id, keyword_id) VALUES (%s,%s)", (movie_id, kid))


def insert_spoken(cur, movie_id, text):
    if pd.isna(text) or text == "":
        return
    for item in str(text).split(","):
        item = item.strip()
        if item:
            code = ensure_language(cur, item)
            cur.execute(
                "INSERT IGNORE INTO movie_language(movie_id, language_code) VALUES (%s,%s)",
                (movie_id, code)
            )


# ==========================
# MAIN IMPORT
# ==========================
def import_csv():
    conn = connect()
    cur = cursor(conn)

    print("Importing ISO languages...")
    import_iso_languages(cur)
    load_valid_languages(cur)
    load_synthetic_counter(cur)

    print("Loading movie CSV...")
    df = pd.read_csv(CSV_MOVIE)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date

    for _, row in df.iterrows():
        movie_id = int(row["id"])

        insert_movie(cur, row)
        insert_genres(cur, movie_id, row["genres"])
        insert_companies(cur, movie_id, row["production_companies"])
        insert_countries(cur, movie_id, row["production_countries"])
        insert_keywords(cur, movie_id, row["keywords"])
        insert_spoken(cur, movie_id, row["spoken_languages"])

    conn.commit()
    cur.close()
    conn.close()
    print("DONE IMPORTING CSV!")


if __name__ == "__main__":
    import_csv()
