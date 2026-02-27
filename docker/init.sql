CREATE TABLE people (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  department TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO people (name, email, department)
SELECT
  'Person ' || gs.id,
  'person' || gs.id || '@example.com',
  CASE (gs.id % 5)
    WHEN 0 THEN 'Engineering'
    WHEN 1 THEN 'Marketing'
    WHEN 2 THEN 'Sales'
    WHEN 3 THEN 'Support'
    WHEN 4 THEN 'Finance'
  END
FROM generate_series(1, 100000) AS gs(id);
