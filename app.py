import os
import re
import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    host = (os.getenv("DB_HOST") or "").strip()
    if host.startswith("http://") or host.startswith("https://"):
        host = host.split("://", 1)[1]
    host = host.split("/", 1)[0].strip().strip(".")

    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=host,
        port=str(os.getenv("DB_PORT", "5432")),
        sslmode=os.getenv("DB_SSLMODE", "require"),
    )


INDEX_AUTHORS = """
CREATE INDEX IF NOT EXISTS idx_books_authors_rating
ON books (authors, average_rating)
"""

INDEX_NUM_PAGES = """
CREATE INDEX IF NOT EXISTS idx_books_num_pages
ON books (num_pages)
"""

INDEX_RATING = """
CREATE INDEX IF NOT EXISTS idx_books_average_rating
ON books (average_rating)
"""


def search_by_author(author):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT title, authors, average_rating
                FROM books
                WHERE authors ILIKE %s
                ORDER BY average_rating DESC
                """,
                (f"%{author}%",),
            )
            return cur.fetchall()


def filter_by_num_pages(min_pages, max_pages):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT title, authors, num_pages
                FROM books
                WHERE num_pages BETWEEN %s AND %s
                ORDER BY num_pages ASC
                """,
                (min_pages, max_pages),
            )
            return cur.fetchall()


def top_rated_books(limit):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT title, authors, average_rating
                FROM books
                ORDER BY average_rating DESC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def explain_query(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("EXPLAIN ANALYZE " + sql, params)
            rows = cur.fetchall()
            return "\n".join(row[0] for row in rows)


def parse_explain(plain_text):
    results = {}

    # Look for the scan type in the Explain
    if "Index Scan" in plain_text:
        results["scan_type"] = "Index Scan"
    elif "Bitmap Heap Scan" in plain_text:
        results["scan_type"] = "Bitmap Heap Scan"
    else:
        results["scan_type"] = "Sequential Scan"

    # Look for the Execution time in the Explain
    time_match = re.search(r"Execution Time: ([\d.]+) ms", plain_text)
    results["execution_time"] = float(time_match.group(1)) if time_match else None

    # Predicted costs and rows
    first_line = plain_text.splitlines()[0] if plain_text else ""
    cost_match = re.search(r"cost=([\d.]+)\.\.([\d.]+)", first_line)
    rows_match = re.search(r"rows=(\d+)", first_line)
    results["startup_cost"] = float(cost_match.group(1)) if cost_match else None
    results["total_cost"] = float(cost_match.group(2)) if cost_match else None
    results["estimated_rows"] = int(rows_match.group(1)) if rows_match else None

    # Pull the actual rows returned
    actual_match = re.search(r"Actual Time: [\d.]+\.\.[\d.]+ rows=(\d+)", plain_text)
    results["actual_rows"] = int(actual_match.group(1)) if actual_match else None

    return results


def compare_indexes(sql, params, index_sql):
    before_plan = explain_query(sql, params)
    before = parse_explain(before_plan)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(index_sql)
            conn.commit()

    after_plan = explain_query(sql, params)
    after = parse_explain(after_plan)

    return {
        "before": before,
        "after": after,
        "before_plan": before_plan,
        "after_plan": after_plan,
    }

def selectivity_sweep(thresholds):
    sql = """SELECT title, authors, average_rating FROM books WHERE average_rating >= %s"""

    results = []
    for t in thresholds:
      plan_text = explain_query(sql, (t,))
      parsed = parse_explain(plan_text)
      parsed["threshold"] = t
      parsed["plan_text"] = plan_text
      results.append(parsed)
    return results


def rows_to_df(rows, columns):
    return pd.DataFrame(rows, columns=columns)


st.set_page_config(page_title="Books Explorer", layout="wide")
st.title("Books Explorer")
st.caption("Search, filter, top-rated list, and query-plan comparison.")

with st.sidebar:
    st.subheader("Database Status")
    if st.button("Test Connection"):
        try:
            with get_conn():
                pass
            st.success("Connected")
        except Exception as e:
            st.error(f"Connection failed: {e}")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Author Search", "Page Filter", "Top Rated", "Explain + Index", "Selectivity Demo"]
)

with tab1:
    st.subheader("Search by Author")
    author = st.text_input("Author name", value="Rowling")
    if st.button("Run Author Search"):
        rows = search_by_author(author)
        st.dataframe(
            rows_to_df(rows, ["title", "authors", "average_rating"]),
            use_container_width=True,
        )

with tab2:
    st.subheader("Filter by Number of Pages")
    c1, c2 = st.columns(2)
    min_pages = c1.number_input("Min pages", min_value=0, value=100)
    max_pages = c2.number_input("Max pages", min_value=0, value=400)
    if st.button("Run Page Filter"):
        rows = filter_by_num_pages(min_pages, max_pages)
        st.dataframe(
            rows_to_df(rows, ["title", "authors", "num_pages"]),
            use_container_width=True,
        )

with tab3:
    st.subheader("Top Rated Books")
    limit = st.slider("How many books", min_value=5, max_value=100, value=20)
    if st.button("Load Top Rated"):
        rows = top_rated_books(limit)
        st.dataframe(
            rows_to_df(rows, ["title", "authors", "average_rating"]),
            use_container_width=True,
        )

with tab4:
    st.subheader("EXPLAIN ANALYZE and Index Impact")
    query_type = st.selectbox(
        "Choose query",
        ["Author Search", "Page Filter", "Top Rated"],
    )

    if query_type == "Author Search":
        author_exp = st.text_input("Author for explain", value="Rowling")
        sql = """
            SELECT title, authors, average_rating
            FROM books
            WHERE authors ILIKE %s
        """
        params = (f"%{author_exp}%",)
        index_sql = INDEX_AUTHORS

    elif query_type == "Page Filter":
        min_exp = st.number_input("Explain min pages", min_value=0, value=100)
        max_exp = st.number_input("Explain max pages", min_value=0, value=500)
        sql = """
            SELECT title, authors, num_pages
            FROM books
            WHERE num_pages BETWEEN %s AND %s
        """
        params = (min_exp, max_exp)
        index_sql = INDEX_NUM_PAGES

    else:
        limit_exp = st.number_input("Explain top limit", min_value=1, value=20)
        sql = """
            SELECT title, authors, average_rating
            FROM books
            ORDER BY average_rating DESC
            LIMIT %s
        """
        params = (limit_exp,)
        index_sql = INDEX_RATING

    if st.button("Compare Before and After Index"):
        result = compare_indexes(sql, params, index_sql)

        b = result["before"]
        a = result["after"]

        c1, c2 = st.columns(2)
        c1.metric("Before scan", b.get("scan_type", "N/A"))
        c2.metric("After scan", a.get("scan_type", "N/A"))

        c3, c4 = st.columns(2)
        c3.metric("Before execution ms", b.get("execution_time"))
        c4.metric("After execution ms", a.get("execution_time"))

        st.text("Before plan")
        st.code(result["before_plan"], language="sql")
        st.text("After plan")
        st.code(result["after_plan"], language="sql")
with tab 5:
    st.subheader("Selectivity vs Plan Choice")
    st.markdown("**Query:** `SELECT ... FROM books WHERE average_rating >= threshold`")

    min_t = st.number_input("Min Threshold", value=0.0, step=0.5,
                            min_value=0.0, max_value=5.0)
    max_t = st.number_input("Max Threshold", value=5.0, step=0.5,
                            min_value=0.0, max_value=5.0)
    steps = st.slider("Number of threshold steps", min_value=3, max_value=15, value=8)

if st.button("Run Selectivity Sweep"):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(INDEX_RATING)
            cur.execute("ANALYZE books")
            conn.commit()

    thresholds = [
        round(min_t + (max_t - min_t) * i / (steps - 1), 2)
        for i in range(steps)
    ]
    sweep = selectivity_sweep(thresholds)

summary_df = pd.DataFrame([
    {
        "threshold": r["threshold"],
        "scan_type": r["scan_type"],
        "estimated_rows": r["estimated_rows"],
        "actual rows": r["actual_rows"],
        "total_cost": r["total_cost"],
        "execution_ms": r["execution_time"],
    }
    for r in sweep
])
st.dataframe(summary_df, use_container_width=True)

transitions = []
for i in range(1, len(sweep)):
    if sweep[i]["scan_type"] != sweep[i - 1]["scan_type"]:
        transitions_append(
            f"At threshold {sweep[i]['threshold']}: "
            f"{sweep[i-1]['scan_type']} -> {sweep[i]['scan_type']}"
        )
if transitions:
    st.success("**Plan transitions detected:**\n\n" + "\n\n".join(transitions))
else:
    st.info("No plan transition in this range - try a wider threshold span.")

st.markdown("### Inspect individual plan")
pick = st.selectbox(
    "Pick a threshold to see full EXPLAIN output",
    options=[r["threshold"] for r in sweep],
)
chosen = next(r for r in sweep if r["threshold"] == pick)
st.code(chosen["plan_text"], language="sql")
        
    
