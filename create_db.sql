CREATE TABLE IF NOT EXISTS companies
    (id INTEGER PRIMARY KEY,
    company_name TEXT NOT NULL
    );

CREATE TABLE IF NOT EXISTS titles
    (id INTEGER PRIMARY KEY,
    title TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS categories
    (id INTEGER PRIMARY KEY,
    category TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS subcategories
    (id INTEGER PRIMARY KEY,
    category TEXT NOT NULL,
    subcategory TEXT NOT NULL,
    FOREIGN KEY (category)
        REFERENCES categories (category)
    );

CREATE TABLE IF NOT EXISTS geo_area
    (id INTEGER PRIMARY KEY,
    area TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS jobs 
    (id INTEGER PRIMARY KEY,
    jobindex_id TEXT NOT NULL,
    title TEXT NOT NULL, 
    job_url TEXT NOT NULL,
    added TEXT NOT NULL,
    expired TEXT NOT NULL,
    company TEXT NOT NULL,
    job_description TEXT NOT NULL,
    FOREIGN KEY (company)
        REFERENCES companies (company_name),
    FOREIGN KEY (title)
        REFERENCES titles (title)
    );


CREATE TABLE IF NOT EXISTS job_category
    (
        id INTEGER PRIMARY KEY,
        jobindex_id TEXT NOT NULL,
        category TEXT NOT NULL,
        subcategory TEXT NOT NULL,
        FOREIGN KEY (jobindex_id)
            REFERENCES jobs (jobindex_id),
        FOREIGN KEY (category) 
            REFERENCES categories (category),
        FOREIGN KEY (subcategory)
            REFERENCES subcategories (subcategory)
    );

CREATE TABLE IF NOT EXISTS job_area
    (
        id INTEGER PRIMARY KEY,
        jobindex_id TEXT NOT NULL,
        area TEXT NOT NULL,
        FOREIGN KEY (jobindex_id)
            REFERENCES jobs (jobindex_id),
        FOREIGN KEY (area) 
            REFERENCES geo_area (area)
    );