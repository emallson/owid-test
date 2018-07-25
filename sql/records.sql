CREATE TABLE records (
    id BIGSERIAL PRIMARY KEY,
    countryId INTEGER NOT NULL REFERENCES countries(id),
    year INTEGER NOT NULL,
    varId INTEGER NOT NULL REFERENCES variables(id),
    varValue NUMERIC NOT NULL
);

-- a unique index both enforcing the uniqueness of the (country, year,
    -- var) tuples and enabling lookups
CREATE UNIQUE INDEX record_variable_tuple_idx ON records(countryId, year, varId);
-- a couple of indices for slicing by country and by year
CREATE INDEX records_country_idx ON records(countryId);
CREATE INDEX records_year_idx ON records(year);
