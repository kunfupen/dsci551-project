DROP TABLE IF EXISTS books;

CREATE TABLE books (
    bookid INTEGER,
    title TEXT,
    authors TEXT,
    average_rating NUMERIC(3,2),
    isbn TEXT,
    isbn13 TEXT,
    language_code TEXT,
    num_pages INTEGER,
    ratings_count INTEGER,
    text_reviews_count INTEGER,
    publication_date TEXT,
    publisher TEXT
);

-- Indexes used by app.py explain/index comparison
CREATE INDEX IF NOT EXISTS idx_books_authors_rating
    ON books (authors, average_rating);

CREATE INDEX IF NOT EXISTS idx_books_num_pages
    ON books (num_pages);

CREATE INDEX IF NOT EXISTS idx_books_average_rating
    ON books (average_rating);

-- Default CSV path if caller does not pass one
\if :{?csv_path}
\else
\set csv_path 'data/books_clean.csv'
\endif

\copy books (
    bookid, title, authors, average_rating, isbn, isbn13,
    language_code, num_pages, ratings_count, text_reviews_count,
    publication_date, publisher
)
FROM :'csv_path'
WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');

ANALYZE books;