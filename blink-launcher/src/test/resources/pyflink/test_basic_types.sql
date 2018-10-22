--  Licensed to the Apache Software Foundation (ASF) under one
--  or more contributor license agreements.  See the NOTICE file
--  distributed with this work for additional information
--  regarding copyright ownership.  The ASF licenses this file
--  to you under the Apache License, Version 2.0 (the
--  "License"); you may not use this file except in compliance
--  with the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
-- limitations under the License.

CREATE FUNCTION integer_udf AS 'python test_basic_types.integer_udf';
CREATE FUNCTION varchar_udf AS 'python test_basic_types.varchar_udf';
CREATE FUNCTION bigint_udf AS 'python test_basic_types.bigint_udf';
CREATE FUNCTION tinyint_udf AS 'python test_basic_types.tinyint_udf';
CREATE FUNCTION smallint_udf AS 'python test_basic_types.smallint_udf';
CREATE FUNCTION float_udf AS 'python test_basic_types.float_udf';
CREATE FUNCTION double_udf AS 'python test_basic_types.double_udf';
CREATE FUNCTION decimal_udf AS 'python test_basic_types.decimal_udf';
CREATE FUNCTION boolean_udf AS 'python test_basic_types.boolean_udf';
CREATE FUNCTION date_udf AS 'python test_basic_types.date_udf';
CREATE FUNCTION time_udf AS 'python test_basic_types.time_udf';
CREATE FUNCTION timestamp_udf AS 'python test_basic_types.timestamp_udf';
CREATE FUNCTION varbinary_udf AS 'python test_basic_types.varbinary_udf';


CREATE TABLE test_source(
    varchar_v VARCHAR(10)
    ,integer_v INTEGER
    ,boolean_v BOOLEAN
    ,tinyint_v TINYINT
    ,smallint_v SMALLINT
    ,bigint_v BIGINT
    ,float_v FLOAT
    ,double_v DOUBLE
    ,decimal_v DECIMAL(20,8)
    ,date_v DATE
    ,time_v TIME
    ,timestamp_v TIMESTAMP
--  ,varbinary_v VARBINARY
)
WITH (
   type='CSV',
   `path`='TEST_CSV_FILEPATH'  -- to be replaced by test before running
);

CREATE TABLE print_sink(
    varchar_v VARCHAR(10)
    ,integer_v INTEGER
    ,boolean_v BOOLEAN
    ,tinyint_v TINYINT
    ,smallint_v SMALLINT
    ,bigint_v BIGINT
    ,float_v FLOAT
    ,double_v DOUBLE
    ,decimal_v DECIMAL(20,8)
    ,date_v DATE
    ,time_v TIME
    ,timestamp_v TIMESTAMP
--  ,varbinary_v VARBINARY
) WITH (
    type = 'TESTSINK'
);


CREATE VIEW left_view (varchar_v, integer_v, boolean_v, tinyint_v, smallint_v,
                       bigint_v, float_v, double_v, decimal_v, date_v, time_v, timestamp_v) AS
SELECT
     varchar_udf(varchar_v)
    ,MAX(integer_udf(integer_v))
    ,MAX(boolean_v)
    ,MAX(tinyint_v)
    ,MAX(smallint_v)
    ,MAX(bigint_v)
    ,MAX(float_v)
    ,MAX(double_v)
    ,MAX(decimal_v)
    ,MAX(date_v)
    ,MAX(time_v)
    ,MAX(timestamp_v)
--  ,varbinary_v VARBINARY
FROM test_source
GROUP BY varchar_udf(varchar_v)
HAVING integer_udf(MAX(integer_v)) > 0 ;


CREATE VIEW right_view (varchar_v, integer_v, boolean_v, tinyint_v, smallint_v,
                        bigint_v, float_v, double_v, decimal_v, date_v, time_v, timestamp_v) AS
SELECT
    varchar_udf(varchar_v)
    ,integer_udf(integer_v)
    ,boolean_udf(boolean_v)
    ,tinyint_udf(tinyint_v)
    ,smallint_udf(smallint_v)
    ,bigint_udf(bigint_v)
    ,float_udf(float_v)
    ,double_udf(double_v)
    ,decimal_udf(decimal_v)
    ,date_udf(date_v)
    ,time_udf(time_v)
    ,timestamp_udf(timestamp_v)
--  ,varbinary_v VARBINARY
FROM left_view
WHERE float_udf(float_v) > 0 ;


INSERT INTO print_sink
SELECT
    varchar_udf(left_view.varchar_v)
    ,integer_udf(right_view.integer_v)
    ,boolean_udf(left_view.boolean_v)
    ,tinyint_udf(right_view.tinyint_v)
    ,smallint_udf(left_view.smallint_v)
    ,bigint_udf(right_view.bigint_v)
    ,float_udf(left_view.float_v)
    ,double_udf(right_view.double_v)
    ,decimal_udf(left_view.decimal_v)
    ,date_udf(right_view.date_v)
    ,time_udf(left_view.time_v)
    ,timestamp_udf(right_view.timestamp_v)
    --,varbinary_udf(varbinary_v)
FROM left_view  JOIN right_view ON left_view.varchar_v = right_view.varchar_v
