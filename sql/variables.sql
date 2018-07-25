CREATE TABLE variables (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE UNIQUE INDEX variables_name_idx ON variables(name);
