PRAGMA foreign_keys = ON;

DROP VIEW IF EXISTS v_student_gpa;
DROP TABLE IF EXISTS enrollments;
DROP TABLE IF EXISTS sections;
DROP TABLE IF EXISTS courses;
DROP TABLE IF EXISTS instructors;
DROP TABLE IF EXISTS students;
DROP TABLE IF EXISTS departments;

CREATE TABLE departments (
    department_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    building TEXT NOT NULL
);

CREATE TABLE students (
    student_id INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    department_id INTEGER NOT NULL,
    class_level TEXT NOT NULL CHECK (class_level IN ('Freshman', 'Sophomore', 'Junior', 'Senior', 'Graduate')),
    start_year INTEGER NOT NULL CHECK (start_year BETWEEN 2015 AND 2035),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

CREATE TABLE instructors (
    instructor_id INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    department_id INTEGER NOT NULL,
    rank TEXT NOT NULL CHECK (rank IN ('Lecturer', 'Assistant Professor', 'Associate Professor', 'Professor')),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

CREATE TABLE courses (
    course_id INTEGER PRIMARY KEY,
    course_code TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    credits INTEGER NOT NULL CHECK (credits BETWEEN 1 AND 5),
    level INTEGER NOT NULL CHECK (level BETWEEN 100 AND 700),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

CREATE TABLE sections (
    section_id INTEGER PRIMARY KEY,
    course_id INTEGER NOT NULL,
    instructor_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    section_label TEXT NOT NULL,
    room TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity BETWEEN 10 AND 200),
    UNIQUE(course_id, term, section_label),
    FOREIGN KEY (course_id) REFERENCES courses(course_id),
    FOREIGN KEY (instructor_id) REFERENCES instructors(instructor_id)
);

CREATE TABLE enrollments (
    enrollment_id INTEGER PRIMARY KEY,
    student_id INTEGER NOT NULL,
    section_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'Enrolled' CHECK (status IN ('Enrolled', 'Dropped', 'Completed')),
    numeric_grade REAL CHECK (numeric_grade IS NULL OR (numeric_grade >= 0 AND numeric_grade <= 100)),
    enrolled_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(student_id, section_id),
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (section_id) REFERENCES sections(section_id)
);

CREATE INDEX idx_students_department ON students(department_id);
CREATE INDEX idx_courses_department ON courses(department_id);
CREATE INDEX idx_sections_term ON sections(term);
CREATE INDEX idx_sections_course ON sections(course_id);
CREATE INDEX idx_enrollments_student ON enrollments(student_id);
CREATE INDEX idx_enrollments_section_status ON enrollments(section_id, status);

CREATE TRIGGER trg_enrollment_capacity_guard
BEFORE INSERT ON enrollments
FOR EACH ROW
WHEN NEW.status IN ('Enrolled', 'Completed')
BEGIN
    SELECT
        CASE
            WHEN (
                SELECT COUNT(*)
                FROM enrollments e
                WHERE e.section_id = NEW.section_id
                  AND e.status IN ('Enrolled', 'Completed')
            ) >= (
                SELECT s.capacity
                FROM sections s
                WHERE s.section_id = NEW.section_id
            )
            THEN RAISE(ABORT, 'Section capacity reached')
        END;
END;

CREATE VIEW v_student_gpa AS
SELECT
    s.student_id,
    s.full_name,
    d.name AS department,
    ROUND(AVG(
        CASE
            WHEN e.numeric_grade IS NULL THEN NULL
            WHEN e.numeric_grade >= 93 THEN 4.0
            WHEN e.numeric_grade >= 90 THEN 3.7
            WHEN e.numeric_grade >= 87 THEN 3.3
            WHEN e.numeric_grade >= 83 THEN 3.0
            WHEN e.numeric_grade >= 80 THEN 2.7
            WHEN e.numeric_grade >= 77 THEN 2.3
            WHEN e.numeric_grade >= 73 THEN 2.0
            WHEN e.numeric_grade >= 70 THEN 1.7
            WHEN e.numeric_grade >= 67 THEN 1.3
            WHEN e.numeric_grade >= 63 THEN 1.0
            WHEN e.numeric_grade >= 60 THEN 0.7
            ELSE 0.0
        END
    ), 2) AS gpa,
    COUNT(e.enrollment_id) AS completed_courses
FROM students s
JOIN departments d ON d.department_id = s.department_id
LEFT JOIN enrollments e
    ON e.student_id = s.student_id
   AND e.status = 'Completed'
   AND e.numeric_grade IS NOT NULL
GROUP BY s.student_id, s.full_name, d.name;
