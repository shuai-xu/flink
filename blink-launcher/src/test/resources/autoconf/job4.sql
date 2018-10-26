
CREATE TABLE originData(
   `catalog` VARCHAR,
   `merge` VARCHAR,
   `domain` VARCHAR,
   nodegroup VARCHAR,
   timestp BIGINT,
   self_fbt DOUBLE,
   swift_error DOUBLE,
   swift_selferror DOUBLE,
   swiftdomain_c200 DOUBLE
  )
   WITH
  (
    type='testsource'
 ) ;

CREATE TABLE status_table_gb_domain_group(
   status VARCHAR,
   `time` BIGINT,
   merge_count BIGINT,
   self_fbt DOUBLE,
   swift_error DOUBLE,
   swift_selferror DOUBLE,
   swiftdomain_c200 DOUBLE,
   PRIMARY KEY(status,`time`))
   WITH
  (
   type='testsink'
 ) ;


INSERT INTO status_table_gb_domain_group
 SELECT
   concat('catalog:' , t.`catalog` , '#domain:' , t.`domain` , '#group:'),
   t.timestp,
   `count`(t.self_fbt),
   `sum`(t.self_fbt),
   `sum`(t.swift_error),
   `sum`(t.swift_selferror),
   `sum`(t.swiftdomain_c200)
 FROM
    originData t
  GROUP BY
    t.`catalog`, t.`domain`, t.timestp;


CREATE TABLE status_table_gb_domainType_group(
   status VARCHAR,
   `time` BIGINT,
   merge_count BIGINT,
   self_fbt DOUBLE,
   swift_error DOUBLE,
   swift_selferror DOUBLE,
   swiftdomain_c200 DOUBLE,
   PRIMARY KEY(status,`time`))
   WITH
  (
   type='testsink'
 ) ;


INSERT INTO status_table_gb_domainType_group
 SELECT
   concat('catalog:' , t.`catalog`),
   t.timestp,
   `count`(t.self_fbt),
   `sum`(t.self_fbt),
   `sum`(t.swift_error),
   `sum`(t.swift_selferror),
   `sum`(t.swiftdomain_c200)
 FROM
    originData t
  GROUP BY
    t.`catalog`,t.timestp;


CREATE TABLE status_table_gb_aliuid_group(
   status VARCHAR,
   `time` BIGINT,
   merge_count BIGINT,
   self_fbt DOUBLE,
   swift_error DOUBLE,
   swift_selferror DOUBLE,
   swiftdomain_c200 DOUBLE,
   PRIMARY KEY(status,`time`))
   WITH
  (
   type='testsink'
 ) ;

 INSERT INTO status_table_gb_aliuid_group
 SELECT
   concat('catalog:' , t.`catalog`),
   t.timestp,
   `count`(t.self_fbt),
   `sum`(t.self_fbt),
   `sum`(t.swift_error),
   `sum`(t.swift_selferror),
   `sum`(t.swiftdomain_c200)
 FROM
 originData t
  GROUP BY t.`catalog`,t.timestp;


CREATE TABLE status_table_gb_domain(
   status VARCHAR,
   `time` BIGINT,
   merge_count BIGINT,
   self_fbt DOUBLE,
   swift_error DOUBLE,
   swift_selferror DOUBLE,
   swiftdomain_c200 DOUBLE,
   PRIMARY KEY(status,`time`))
   WITH
  (
   type='testsink'
 ) ;

INSERT INTO status_table_gb_domain
 SELECT
   concat('catalog:' , t.`catalog`),
   t.timestp,
   `count`(t.self_fbt),
   `sum`(t.self_fbt),
   `sum`(t.swift_error),
   `sum`(t.swift_selferror),
   `sum`(t.swiftdomain_c200)
 FROM
 originData t
  GROUP BY t.`catalog` , t.timestp;


CREATE TABLE output_table(
   `catalog` VARCHAR,
   `merge` VARCHAR,
   `domain` VARCHAR,
   nodegroup VARCHAR,
   timestp BIGINT,
   self_fbt DOUBLE,
   swift_error DOUBLE,
   swift_selferror DOUBLE,
   swiftdomain_c200 DOUBLE)
   WITH
  (
   type='testsink'
 ) ;

INSERT INTO output_table
 SELECT
   last_value(`catalog`),
   last_value(`merge`),
   last_value(`domain`),
   last_value(nodegroup),
   last_value(timestp),
   last_value(self_fbt),
   last_value(swift_error),
   last_value(swift_selferror),
   last_value(swiftdomain_c200)
 FROM
    originData t
 group by `catalog`,
    `merge`,
    `domain`,
    nodegroup,
    timestp,
    self_fbt,
    swift_error,
    swift_selferror,
    swiftdomain_c200;
