-- Creation de la table linkedin
CREATE DATABASE if not exists linkedin;

-- Utilisation de la base de donnée
use database linkedin;
use schema PUBLIC;

-- Création du Stage externe linkedin pour récupérer les finchiers dans le S3 proposé par AWS

create or replace stage linkedin_stage
  url='s3://snowflake-lab-bucket/';

  -- Lister le contenu du stage :
  list @linkedin_stage;

   -- Définition des formats de fichiers : Spécifiez les formats appropriés pour les fichiers CSV 
create or replace file format csv
  type = 'CSV'
  field_delimiter = ','
  record_delimiter = '\n'
  skip_header = 1
  field_optionally_enclosed_by = '\042'
  null_if = (''); 
  
  -- Contrôle des fichiers CSV après le formatage
  show file formats in database linkedin;
  
  -- Définition des formats de fichiers : Spécifiez les formats appropriés pour les fichiers JSON.
create or replace file format json
  type = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = FALSE
  IGNORE_UTF8_ERRORS = FALSE
  SKIP_BYTE_ORDER_MARK = TRUE
  ENABLE_OCTAL = FALSE
  ALLOW_DUPLICATE = FALSE
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  TRIM_SPACE = FALSE
  NULL_IF = ('');
  
  -- Contrôle des fichiers JSON après le formatage
  show file formats in database linkedin;

 -- Création de la TABLE job_postings permet de stocker les jobs proposés
CREATE OR REPLACE TABLE job_postings (
    job_id                    NUMBER          NOT NULL PRIMARY KEY,
    company_name              VARCHAR(255),
    title                     VARCHAR(255),
    description               TEXT,
    max_salary                FLOAT,
    med_salary                FLOAT,
    min_salary                FLOAT,
    pay_period                VARCHAR(50), -- Hourly, Monthly, Yearly
    formatted_work_type       VARCHAR(50), -- Fulltime, Parttime, Contract
    location                  VARCHAR(255),
    applies                   NUMBER,
    original_listed_time      TIMESTAMP_NTZ,
    remote_allowed            BOOLEAN,
    views                     NUMBER,
    job_posting_url           VARCHAR(1000),
    application_url           VARCHAR(1000),
    application_type          VARCHAR(100),       -- offsite, complex/simple onsite
    expiry                    TIMESTAMP_NTZ,
    closed_time               TIMESTAMP_NTZ,
    formatted_experience_level VARCHAR(100),      -- entry, associate, executive, etc
    skills_desc               TEXT,
    listed_time               TIMESTAMP_NTZ,
    posting_domain            VARCHAR(255),
    sponsored                 BOOLEAN,
    work_type                 VARCHAR(100),
    currency                  VARCHAR(10),
    compensation_type         VARCHAR(100)
);

-- Vérifier si la table est créée
Show tables;


-- Création de la TABLE benefits de lister les avantages du Job

CREATE OR REPLACE TABLE benefits (
    job_id      NUMBER           NOT NULL,
    inferred    BOOLEAN,
    type        VARCHAR(100),    -- 401K, Medical Insurance, etc
    CONSTRAINT fk_benefits_job FOREIGN KEY (job_id) REFERENCES job_postings(job_id)
);

-- Vérifier si la table est créée
Show tables like 'BENEFITS';


-- Création de la TABLE companies qui décrit les sociétés qui proposent le Job
CREATE OR REPLACE TABLE companies (
    company_id    NUMBER          NOT NULL PRIMARY KEY,
    name          VARCHAR(255),
    description   TEXT,
    company_size  NUMBER,          -- 0 Smallest - 7 Largest
    state         VARCHAR(100),
    country       VARCHAR(100),
    city          VARCHAR(100),
    zip_code      VARCHAR(20),
    address       VARCHAR(500),
    url           VARCHAR(1000)
);

-- Création de la TABLE employee_counts
CREATE OR REPLACE TABLE employee_counts (
    company_id      NUMBER          NOT NULL,
    employee_count  NUMBER,
    follower_count  NUMBER,
    time_recorded   NUMBER,                       -- Unix timestamp
    CONSTRAINT fk_emp_company FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- Création de la TABLE job_skills
CREATE OR REPLACE TABLE job_skills (
    job_id      NUMBER           NOT NULL,
    skill_abr   VARCHAR(100)     NOT NULL,
    CONSTRAINT pk_job_skills PRIMARY KEY (job_id, skill_abr),
    CONSTRAINT fk_skills_job FOREIGN KEY (job_id) REFERENCES job_postings(job_id)
);


-- Création de la TABLE TABLE  job_industries
CREATE OR REPLACE TABLE job_industries (
    job_id       NUMBER          NOT NULL,
    industry_id  NUMBER          NOT NULL,
    CONSTRAINT pk_job_industries PRIMARY KEY (job_id, industry_id),
    CONSTRAINT fk_industries_job FOREIGN KEY (job_id) REFERENCES job_postings(job_id)
);

-- Création de la TABLE company_specialities
CREATE OR REPLACE TABLE company_specialities (
    company_id  NUMBER          NOT NULL,
    speciality  VARCHAR(255)    NOT NULL,
    CONSTRAINT pk_company_specialities PRIMARY KEY (company_id, speciality),
    CONSTRAINT fk_spec_company FOREIGN KEY (company_id) REFERENCES companies(company_id)
);


-- Création de la TABLE company_industries
CREATE OR REPLACE TABLE company_industries (
    company_id   NUMBER          NOT NULL,
    industry     VARCHAR(255)    NOT NULL,
    CONSTRAINT pk_company_industries PRIMARY KEY (company_id, industry),
    CONSTRAINT fk_ind_company FOREIGN KEY (company_id) REFERENCES companies(company_id)
);


-- Chargement des données : Utilisez la commande COPY INTO pour importer les données depuis le stage.

-- créaton des vues pour nettoyage avant insertion (données brutes avant transformation)  pour les fichiers json et csv 

-- Staging JSON companies
CREATE OR REPLACE VIEW stg_companies AS
SELECT $1::VARIANT AS raw
FROM @linkedin_stage/companies.json
(FILE_FORMAT => json);


-- Staging JSON company_industries
CREATE OR REPLACE VIEW stg_company_industries AS
SELECT $1::VARIANT AS raw
FROM @linkedin_stage/company_industries.json
(FILE_FORMAT => json);

 
-- Staging JSON company_specialities
CREATE OR REPLACE VIEW stg_company_specialities AS
SELECT $1::VARIANT AS raw
FROM @linkedin_stage/company_specialities.json
(FILE_FORMAT => json);

-- Staging JSON job_industries
CREATE OR REPLACE VIEW stg_job_industries AS
SELECT $1::VARIANT AS raw
FROM @linkedin_stage/job_industries.json
(FILE_FORMAT => json);

-- Staging csv stg_job_postings
CREATE OR REPLACE VIEW stg_job_postings AS
SELECT
    $1::NUMBER          AS job_id,
    $2::VARCHAR         AS company_name,
    $3::VARCHAR         AS title,
    $4::VARCHAR         AS description,
    $5::FLOAT           AS max_salary,
    $6::FLOAT           AS med_salary,
    $7::FLOAT           AS min_salary,
    $8::VARCHAR         AS pay_period,
    $9::VARCHAR         AS formatted_work_type,
    $10::VARCHAR        AS location,
    $11::NUMBER         AS applies,
    $12::NUMBER         AS original_listed_time,
    $13::BOOLEAN        AS remote_allowed,
    $14::NUMBER         AS views,
    $15::VARCHAR        AS job_posting_url,
    $16::VARCHAR        AS application_url,
    $17::VARCHAR        AS application_type,
    $18::NUMBER         AS expiry,
    $19::NUMBER         AS closed_time,
    $20::VARCHAR        AS formatted_experience_level,
    $21::VARCHAR        AS skills_desc,
    $22::NUMBER         AS listed_time,
    $23::VARCHAR        AS posting_domain,
    $24::BOOLEAN        AS sponsored,
    $25::VARCHAR        AS work_type,
    $26::VARCHAR        AS currency,
    $27::VARCHAR        AS compensation_type
FROM @linkedin_stage/job_postings.csv
(FILE_FORMAT => csv);

-- Staging csv BENEFITS
CREATE OR REPLACE VIEW stg_benefits AS
SELECT
    $1::NUMBER          AS job_id,
    $2::BOOLEAN         AS inferred,
    $3::VARCHAR         AS type
FROM @linkedin_stage/benefits.csv
(FILE_FORMAT => csv);

-- Staging csv EMPLOYEE_COUNTS
CREATE OR REPLACE VIEW stg_employee_counts AS
SELECT
    $1::NUMBER          AS company_id,
    $2::NUMBER          AS employee_count,
    $3::NUMBER          AS follower_count,
    $4::NUMBER          AS time_recorded
FROM @linkedin_stage/employee_counts.csv
(FILE_FORMAT => csv);

-- Staging csv JOB_SKILLS
CREATE OR REPLACE VIEW stg_job_skills AS
SELECT
    $1::NUMBER          AS job_id,
    $2::VARCHAR         AS skill_abr
FROM @linkedin_stage/job_skills.csv
(FILE_FORMAT => csv);

-- CHARGEMENT DES TABLES CIBLES DEPUIS LES VUES DE STAGING

-- companies
INSERT INTO companies (
    company_id, name, description, company_size,
    state, country, city, zip_code, address, url
)
SELECT
    raw:company_id::NUMBER AS company_id,

    TRIM(raw:name::VARCHAR) AS name,

    TRIM(raw:description::VARCHAR) AS description,

    raw:company_size::NUMBER AS company_size,

    TRIM(raw:state::VARCHAR) AS state,

    TRIM(raw:country::VARCHAR) AS country,

    TRIM(raw:city::VARCHAR) AS city,

    CASE 
        WHEN REGEXP_LIKE(TRIM(raw:zip_code::VARCHAR), '^[0-9A-Za-z -]{3,10}$')
        THEN TRIM(raw:zip_code::VARCHAR)
        ELSE NULL
    END AS zip_code,

    TRIM(raw:address::VARCHAR) AS address,

    TRIM(raw:url::VARCHAR) AS url

FROM stg_companies
WHERE raw:company_id IS NOT NULL;

-- COMPANY_INDUSTRIES 
INSERT INTO company_industries (company_id, industry)
SELECT
    raw:company_id::NUMBER              AS company_id,
    TRIM(raw:industry::VARCHAR)         AS industry
FROM stg_company_industries
WHERE raw:company_id IS NOT NULL
  AND raw:industry IS NOT NULL;

-- COMPANY_SPECIALITIES
INSERT INTO company_specialities (company_id, speciality)
SELECT
    raw:company_id::NUMBER AS company_id,
    TRIM(value::VARCHAR) AS speciality
FROM stg_company_specialities,
LATERAL FLATTEN(input => SPLIT(raw:speciality::VARCHAR, ','))
WHERE raw:company_id IS NOT NULL
  AND raw:speciality IS NOT NULL
  AND LENGTH(TRIM(value::VARCHAR)) <= 100;

  -- JOB_INDUSTRIES 
INSERT INTO job_industries (job_id, industry_id)
SELECT
    raw:job_id::NUMBER                  AS job_id,
    TRIM(raw:industry_id::VARCHAR)      AS industry_id
FROM stg_job_industries
WHERE raw:job_id IS NOT NULL
  AND raw:industry_id IS NOT NULL;

-- JOB_POSTINGS 
INSERT INTO job_postings (
    job_id, company_name, title, description,
    max_salary, med_salary, min_salary, pay_period,
    formatted_work_type, location, applies,
    original_listed_time, views,
    job_posting_url, application_url, application_type,
    expiry, closed_time, formatted_experience_level,
    skills_desc, listed_time, posting_domain,
    work_type, currency, compensation_type
)
SELECT
    job_id,
    TRIM(company_name),
    TRIM(title),
    TRIM(description),
    max_salary,
    med_salary,
    min_salary,
    INITCAP(TRIM(pay_period)),
    INITCAP(TRIM(formatted_work_type)),
    TRIM(location),
    applies,
    TO_TIMESTAMP_NTZ(original_listed_time),
    views,
    CASE WHEN job_posting_url ILIKE 'http%' THEN job_posting_url ELSE NULL END,
    CASE WHEN application_url ILIKE 'http%' THEN application_url ELSE NULL END,
    LOWER(TRIM(application_type)),
    TO_TIMESTAMP_NTZ(expiry),
    TO_TIMESTAMP_NTZ(closed_time),
    INITCAP(TRIM(formatted_experience_level)),
    TRIM(skills_desc),
    TO_TIMESTAMP_NTZ(listed_time),
    TRIM(posting_domain),
    INITCAP(TRIM(work_type)),
    UPPER(TRIM(currency)),
    LOWER(TRIM(compensation_type))
FROM stg_job_postings
WHERE job_id IS NOT NULL;

-- BENEFITS 
INSERT INTO benefits (job_id, inferred, type)
SELECT
    job_id,
    inferred,
    TRIM(type)
FROM stg_benefits
WHERE job_id IS NOT NULL
  AND job_id IN (SELECT job_id FROM job_postings);

  -- employee_counts 
INSERT INTO employee_counts (company_id, employee_count, follower_count, time_recorded)
SELECT
    company_id,
    employee_count,
    follower_count,
    time_recorded
FROM stg_employee_counts
WHERE company_id IS NOT NULL
  AND company_id IN (SELECT company_id FROM companies);

-- job_skills
INSERT INTO job_skills (job_id, skill_abr)
SELECT
    job_id,
    TRIM(skill_abr)
FROM stg_job_skills
WHERE job_id IS NOT NULL
  AND skill_abr IS NOT NULL
  AND job_id IN (SELECT job_id FROM job_postings);

