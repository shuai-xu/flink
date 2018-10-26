create table custom_input1(
    a BIGINT,
    b BIGINT
)with(
  type='testsource'
);

create table custom_input2(
    c BIGINT,
    d BIGINT
)with(
  type='testsource'
);

create table custom_input3(
    e BIGINT,
    f BIGINT
)with(
  type='testsource'
);


create table print(
  a BIGINT,
  b BIGINT,
  c BIGINT,
  d BIGINT,
  e BIGINT,
  f BIGINT
)with(
    type='testsink'
);

INSERT INTO print
SELECT * FROM (
    SELECT * FROM custom_input1 LEFT OUTER JOIN custom_input2 ON b = d
) LEFT OUTER JOIN custom_input3 ON b = e
