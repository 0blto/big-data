CREATE TABLE dim_city (
    city_sk BIGSERIAL PRIMARY KEY,
    city_bk BIGINT NOT NULL UNIQUE,
    city_name TEXT NOT NULL
);


CREATE TABLE dim_location (
    location_sk BIGSERIAL PRIMARY KEY,
    location_bk BIGINT NOT NULL UNIQUE,
    city_sk BIGINT NOT NULL REFERENCES dim_city(city_sk),
    address TEXT
);

CREATE TABLE dim_company (
    company_sk BIGSERIAL PRIMARY KEY,
    company_bk BIGINT NOT NULL UNIQUE,
    company_name TEXT NOT NULL
);

CREATE TABLE dim_vacancy_name (
    vacancy_name_sk BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    UNIQUE (title, is_current)
);

CREATE TABLE dim_vacancy (
    vacancy_sk BIGSERIAL PRIMARY KEY,
    vacancy_bk BIGINT NOT NULL, -- бизнес-ключ
    vacancy_name_sk BIGINT NOT NULL REFERENCES dim_vacancy_name(vacancy_name_sk),
    company_sk BIGINT NOT NULL REFERENCES dim_company(company_sk),
    vacancy_url TEXT,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    UNIQUE (vacancy_bk, is_current)
);

CREATE UNIQUE INDEX ux_dim_vacancy_current
ON dim_vacancy (vacancy_bk)
WHERE is_current = true;

CREATE TABLE dim_date (
    date_sk BIGSERIAL PRIMARY KEY,
    date_bk DATE NOT NULL UNIQUE,
    day INT,
    month INT,
    quarter INT,
    year INT,
    day_of_week INT
);

CREATE TABLE fact_salary (
    fact_sk BIGSERIAL PRIMARY KEY,
    vacancy_sk BIGINT NOT NULL REFERENCES dim_vacancy(vacancy_sk),
    location_sk BIGINT REFERENCES dim_location(location_sk),
    date_sk BIGINT REFERENCES dim_date(date_sk),
    salary_from INT,
    salary_to INT,
    salary_currency TEXT,
    archived BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fact_salary UNIQUE (vacancy_sk, location_sk, date_sk)
);