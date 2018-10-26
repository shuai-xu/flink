create table custom_input(
  x BIGINT,
  y BIGINT
)with(
  type='testsource'
);

create table print(
  a BIGINT,
  b BIGINT,
  c varchar
)with(
    type='testsink'
);

INSERT INTO print
SELECT
 x, COUNT(y), 'Test'
FROM custom_input
GROUP BY x
