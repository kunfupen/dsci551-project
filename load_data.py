import csv
import re
from pathlib import Path

INPUT_PATH = Path("data/books.csv")
OUTPUT_PATH = Path("data/books_clean.csv")

EXPECTED_COLUMNS = [
    "bookID",
    "title",
    "authors",
    "average_rating",
    "isbn",
    "isbn13",
    "language_code",
    "num_pages",
    "ratings_count",
    "text_reviews_count",
    "publication_date",
    "publisher",
]

TAIL_RE = re.compile(
    r"^(.*),([0-5]\.\d{2}),([^,]+),([^,]+),([^,]+),(\d+),(\d+),(\d+),(\d{1,2}/\d{1,2}/\d{4}),(.*)$"
)


def split_title_authors(front):
    chunks = front.split(",")
    if len(chunks) < 2:
        return None, None

    title = ",".join(chunks[:-1]).strip()
    authors = chunks[-1].strip()

    if len(chunks) >= 3 and authors.startswith(("Jr.", "Sr.", "II", "III", "IV", "V")):
        title = ",".join(chunks[:-2]).strip()
        authors = (chunks[-2] + "," + chunks[-1]).strip()

    return title, authors


def clean_line(raw_line):
    raw = raw_line.rstrip("\n")
    if not raw:
        return None

    if "," not in raw:
        return None

    bookid, rest = raw.split(",", 1)
    bookid = bookid.strip()
    if not bookid.isdigit():
        return None

    m = TAIL_RE.match(rest)
    if not m:
        return None

    front = m.group(1).strip()
    average_rating = m.group(2).strip()
    isbn = m.group(3).strip()
    isbn13 = m.group(4).strip()
    language_code = m.group(5).strip()
    num_pages = m.group(6).strip()
    ratings_count = m.group(7).strip()
    text_reviews_count = m.group(8).strip()
    publication_date = m.group(9).strip()
    publisher = m.group(10).strip()

    title, authors = split_title_authors(front)
    if not title or not authors:
        return None

    return [
        bookid,
        title,
        authors,
        average_rating,
        isbn,
        isbn13,
        language_code,
        num_pages,
        ratings_count,
        text_reviews_count,
        publication_date,
        publisher,
    ]


def main():
    total = 0
    cleaned = 0
    skipped = 0

    with INPUT_PATH.open("r", encoding="utf-8", newline="") as fin, \
         OUTPUT_PATH.open("w", encoding="utf-8", newline="") as fout:

        writer = csv.writer(fout, quoting=csv.QUOTE_ALL)

        _header = fin.readline().strip().split(",")
        writer.writerow(EXPECTED_COLUMNS)

        for line in fin:
            total += 1
            row = clean_line(line)
            if row is None:
                skipped += 1
                continue
            writer.writerow(row)
            cleaned += 1

    print(f"Total rows read: {total}")
    print(f"Rows cleaned: {cleaned}")
    print(f"Rows skipped: {skipped}")
    print(f"Wrote cleaned CSV to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()