#!/usr/bin/env python3
"""Initialize and seed the DSCI 551 demo SQLite database."""

from __future__ import annotations

import argparse
import random
import sqlite3
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = ROOT_DIR / "schema.sql"
DEFAULT_DB_PATH = ROOT_DIR / "dsci551_demo.db"

FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Sam", "Riley", "Casey", "Morgan", "Avery", "Drew", "Logan",
    "Cameron", "Parker", "Quinn", "Rowan", "Sage", "Reese", "Hayden", "Dakota", "Skyler", "Emerson",
]

LAST_NAMES = [
    "Nguyen", "Patel", "Kim", "Garcia", "Smith", "Chen", "Johnson", "Williams", "Brown", "Davis",
    "Martinez", "Lee", "Anderson", "Thomas", "Hernandez", "Moore", "Martin", "Jackson", "White", "Clark",
]

DEPARTMENTS: Sequence[Tuple[str, str]] = [
    ("Data Science", "SCI-101"),
    ("Computer Science", "ENG-210"),
    ("Mathematics", "MTH-330"),
    ("Business Analytics", "BUS-220"),
    ("Economics", "ECO-120"),
]

COURSE_BLUEPRINTS: Sequence[Tuple[str, str, str, int, int]] = [
    ("DSCI 510", "Data Management", "Data Science", 4, 500),
    ("DSCI 551", "Foundations of Data Management", "Data Science", 4, 500),
    ("DSCI 552", "Machine Learning for Data Science", "Data Science", 4, 500),
    ("DSCI 553", "Data Visualization", "Data Science", 4, 500),
    ("CSCI 570", "Algorithms", "Computer Science", 4, 500),
    ("CSCI 585", "Database Systems", "Computer Science", 4, 500),
    ("CSCI 571", "Web Technologies", "Computer Science", 4, 500),
    ("MATH 407", "Probability Theory", "Mathematics", 4, 400),
    ("MATH 425", "Numerical Analysis", "Mathematics", 4, 400),
    ("BUAN 520", "Optimization for Analytics", "Business Analytics", 4, 500),
    ("BUAN 530", "Predictive Analytics", "Business Analytics", 4, 500),
    ("ECON 500", "Econometrics", "Economics", 4, 500),
]

TERMS = ["Spring 2025", "Fall 2025", "Spring 2026"]
CLASS_LEVELS = ["Freshman", "Sophomore", "Junior", "Senior", "Graduate"]
INSTRUCTOR_RANKS = ["Lecturer", "Assistant Professor", "Associate Professor", "Professor"]


class Loader:
    def __init__(self, db_path: Path, seed: int) -> None:
        self.db_path = db_path
        self.random = random.Random(seed)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize_schema(self, conn: sqlite3.Connection) -> None:
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        conn.executescript(schema_sql)

    def seed_departments(self, conn: sqlite3.Connection) -> Dict[str, int]:
        conn.executemany(
            "INSERT INTO departments (name, building) VALUES (?, ?)",
            DEPARTMENTS,
        )
        rows = conn.execute("SELECT department_id, name FROM departments").fetchall()
        return {row["name"]: row["department_id"] for row in rows}

    def _make_name(self, used: set[str]) -> str:
        while True:
            name = f"{self.random.choice(FIRST_NAMES)} {self.random.choice(LAST_NAMES)}"
            if name not in used:
                used.add(name)
                return name

    def seed_instructors(self, conn: sqlite3.Connection, dept_ids: Dict[str, int], count: int) -> None:
        used_names: set[str] = set()
        records = []
        for idx in range(1, count + 1):
            name = self._make_name(used_names)
            dept_name = self.random.choice(list(dept_ids.keys()))
            email = f"{name.lower().replace(' ', '.')}.faculty{idx}@university.edu"
            records.append(
                (
                    name,
                    email,
                    dept_ids[dept_name],
                    self.random.choice(INSTRUCTOR_RANKS),
                )
            )

        conn.executemany(
            """
            INSERT INTO instructors (full_name, email, department_id, rank)
            VALUES (?, ?, ?, ?)
            """,
            records,
        )

    def seed_courses(self, conn: sqlite3.Connection, dept_ids: Dict[str, int]) -> None:
        rows = []
        for code, title, dept_name, credits, level in COURSE_BLUEPRINTS:
            rows.append((code, title, dept_ids[dept_name], credits, level))

        conn.executemany(
            """
            INSERT INTO courses (course_code, title, department_id, credits, level)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )

    def seed_students(self, conn: sqlite3.Connection, dept_ids: Dict[str, int], count: int) -> None:
        used_names: set[str] = set()
        rows = []
        dept_choices = list(dept_ids.items())

        for idx in range(1, count + 1):
            name = self._make_name(used_names)
            dept_name, dept_id = self.random.choice(dept_choices)
            email = f"{name.lower().replace(' ', '.')}.{idx}@student.edu"
            class_level = self.random.choices(
                CLASS_LEVELS,
                weights=[0.12, 0.18, 0.24, 0.2, 0.26],
                k=1,
            )[0]
            start_year = self.random.choice([2022, 2023, 2024, 2025, 2026])
            if class_level == "Graduate":
                start_year = self.random.choice([2024, 2025, 2026])

            rows.append((name, email, dept_id, class_level, start_year))

        conn.executemany(
            """
            INSERT INTO students (full_name, email, department_id, class_level, start_year)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )

    def seed_sections(self, conn: sqlite3.Connection, sections_per_course: int) -> None:
        course_rows = conn.execute("SELECT course_id FROM courses ORDER BY course_id").fetchall()
        instructor_rows = conn.execute("SELECT instructor_id FROM instructors ORDER BY instructor_id").fetchall()
        instructor_ids = [row["instructor_id"] for row in instructor_rows]

        rows = []
        for course_row in course_rows:
            for idx in range(sections_per_course):
                term = TERMS[idx % len(TERMS)]
                label = f"{idx + 1:03d}"
                room = f"RM-{100 + self.random.randint(1, 250)}"
                capacity = self.random.choice([25, 30, 35, 40, 45])
                instructor_id = self.random.choice(instructor_ids)
                rows.append(
                    (
                        course_row["course_id"],
                        instructor_id,
                        term,
                        label,
                        room,
                        capacity,
                    )
                )

        conn.executemany(
            """
            INSERT INTO sections (course_id, instructor_id, term, section_label, room, capacity)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def seed_enrollments(self, conn: sqlite3.Connection, min_courses: int, max_courses: int) -> None:
        students = [row["student_id"] for row in conn.execute("SELECT student_id FROM students").fetchall()]
        section_rows = conn.execute(
            "SELECT section_id, term, capacity FROM sections ORDER BY section_id"
        ).fetchall()

        # Track remaining capacity in Python to avoid failed inserts from trigger.
        remaining_capacity = {row["section_id"]: row["capacity"] for row in section_rows}
        section_ids = [row["section_id"] for row in section_rows]

        enrollment_rows = []
        for student_id in students:
            target_count = self.random.randint(min_courses, max_courses)
            enrolled = 0
            tried = 0
            picked_sections = set()
            while enrolled < target_count and tried < len(section_ids) * 3:
                section_id = self.random.choice(section_ids)
                tried += 1
                if section_id in picked_sections:
                    continue
                if remaining_capacity[section_id] <= 0:
                    continue

                picked_sections.add(section_id)
                remaining_capacity[section_id] -= 1
                status = self.random.choices(
                    ["Enrolled", "Completed", "Dropped"],
                    weights=[0.25, 0.68, 0.07],
                    k=1,
                )[0]
                grade = None
                if status == "Completed":
                    grade = round(self.random.gauss(82, 10), 1)
                    grade = max(45.0, min(100.0, grade))

                enrollment_rows.append((student_id, section_id, status, grade))
                enrolled += 1

        conn.executemany(
            """
            INSERT INTO enrollments (student_id, section_id, status, numeric_grade)
            VALUES (?, ?, ?, ?)
            """,
            enrollment_rows,
        )

    def run(self, students: int, instructors: int, sections_per_course: int) -> None:
        with self.connect() as conn:
            self.initialize_schema(conn)
            dept_ids = self.seed_departments(conn)
            self.seed_instructors(conn, dept_ids, instructors)
            self.seed_courses(conn, dept_ids)
            self.seed_students(conn, dept_ids, students)
            self.seed_sections(conn, sections_per_course)
            self.seed_enrollments(conn, min_courses=2, max_courses=6)

            summary = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM departments) AS departments,
                    (SELECT COUNT(*) FROM instructors) AS instructors,
                    (SELECT COUNT(*) FROM courses) AS courses,
                    (SELECT COUNT(*) FROM students) AS students,
                    (SELECT COUNT(*) FROM sections) AS sections,
                    (SELECT COUNT(*) FROM enrollments) AS enrollments
                """
            ).fetchone()

            print("Database initialized and seeded successfully.")
            print(f"Database file: {self.db_path}")
            print(
                "Rows -> "
                f"departments={summary['departments']}, "
                f"instructors={summary['instructors']}, "
                f"courses={summary['courses']}, "
                f"students={summary['students']}, "
                f"sections={summary['sections']}, "
                f"enrollments={summary['enrollments']}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize and seed DSCI 551 demo database.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite DB file.")
    parser.add_argument("--seed", type=int, default=551, help="Random seed for reproducible data.")
    parser.add_argument("--students", type=int, default=180, help="Number of students to generate.")
    parser.add_argument("--instructors", type=int, default=28, help="Number of instructors to generate.")
    parser.add_argument(
        "--sections-per-course",
        type=int,
        default=3,
        help="How many sections to create for each course.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    loader = Loader(db_path=args.db, seed=args.seed)
    loader.run(
        students=args.students,
        instructors=args.instructors,
        sections_per_course=args.sections_per_course,
    )


if __name__ == "__main__":
    main()
