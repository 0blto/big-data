CREATE TABLE IF NOT EXISTS ods_hh_vacancies (
    id BIGSERIAL PRIMARY KEY,
    vacancy_id TEXT NOT NULL,
    title TEXT,
    employer TEXT,
    city TEXT,
    location TEXT,
    salary_from INT,
    salary_to INT,
    salary_currency TEXT,
    url TEXT,
    archived BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMP,
    raw JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);