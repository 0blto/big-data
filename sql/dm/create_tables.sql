DROP TABLE IF EXISTS dm_vacancy_market;
DROP TABLE IF EXISTS dm_companies_activity;
DROP TABLE IF EXISTS dm_vacancy_titles;

CREATE TABLE dm_vacancy_market AS
SELECT
    d.date_bk               AS date,
    c.city_name             AS city,
    COUNT(DISTINCT fs.vacancy_sk) AS vacancies_cnt,
    AVG(fs.salary_from)     AS avg_salary_from,
    AVG(fs.salary_to)       AS avg_salary_to
FROM fact_salary fs
JOIN dim_date d ON fs.date_sk = d.date_sk
JOIN dim_location l ON fs.location_sk = l.location_sk
JOIN dim_city c ON l.city_sk = c.city_sk
WHERE fs.archived = FALSE
GROUP BY d.date_bk, c.city_name;

CREATE TABLE dm_companies_activity AS
SELECT
    comp.company_name,
    COUNT(DISTINCT fs.vacancy_sk) AS vacancies_cnt,
    AVG(fs.salary_from) AS avg_salary_from,
    AVG(fs.salary_to) AS avg_salary_to
FROM fact_salary fs
JOIN dim_vacancy v ON fs.vacancy_sk = v.vacancy_sk
JOIN dim_company comp ON v.company_sk = comp.company_sk
WHERE fs.archived = FALSE
GROUP BY comp.company_name;

CREATE TABLE dm_vacancy_titles AS
SELECT
    vn.title,
    COUNT(DISTINCT fs.vacancy_sk) AS vacancies_cnt,
    AVG(fs.salary_from) AS avg_salary_from,
    AVG(fs.salary_to) AS avg_salary_to
FROM fact_salary fs
JOIN dim_vacancy v ON fs.vacancy_sk = v.vacancy_sk
JOIN dim_vacancy_name vn
    ON v.vacancy_name_sk = vn.vacancy_name_sk
WHERE vn.is_current = TRUE
  AND fs.archived = FALSE
GROUP BY vn.title;