create table tt_stream1(
  app_id VARCHAR
  ,app_version VARCHAR
  ,app_bu VARCHAR
  ,app_range VARCHAR
  ,app_name VARCHAR
  ,app_key VARCHAR
  ,imei VARCHAR
  ,imsi VARCHAR
  ,imeisi VARCHAR
  ,category_id VARCHAR
  ,account_id VARCHAR
  ,long_login_nick VARCHAR
  ,long_login_user_id VARCHAR
  ,visitor_id VARCHAR
  ,visitor_type VARCHAR
  ,is_active VARCHAR
)with(
  type='testsource'
);

create table tt_stream2(
  app_id VARCHAR
  ,app_version VARCHAR
  ,app_bu VARCHAR
  ,app_range VARCHAR
  ,app_name VARCHAR
  ,app_key VARCHAR
  ,imei VARCHAR
  ,imsi VARCHAR
  ,imeisi VARCHAR
  ,category_id VARCHAR
  ,account_id VARCHAR
  ,long_login_nick VARCHAR
  ,long_login_user_id VARCHAR
  ,visitor_id VARCHAR
  ,visitor_type VARCHAR
  ,is_active VARCHAR
)with(
  type='testsource'
);

create view tmp_view1 as
select
  md5(concat(app_id, app_bu)) as md5_app_id
  ,LAST_VALUE(app_version) as app_version
  ,LAST_VALUE(app_bu) as app_bu
  ,LAST_VALUE(app_range) as app_range
FROM tt_stream1
where SUBSTRING(md5(app_id), 0, 1) = 'a'
group by app_id,app_bu;

create view tmp_view2 as
select
  md5(concat(app_id, app_version)) as md5_app_id
  ,LAST_VALUE(app_version) as app_version
  ,LAST_VALUE(app_bu) as app_bu
  ,LAST_VALUE(app_range) as app_range
FROM tt_stream2
where SUBSTRING(md5(app_id), 0, 1) = 'a'
group by app_id,app_version;

create view union_view as
select
   md5_app_id
   ,app_version
   ,app_bu
   ,app_range
 from tmp_view1
 union ALL
 select
   md5_app_id
   ,app_version
   ,app_bu
   ,app_range
 from tmp_view2;

create table tt_output(
  md5_app_id VARCHAR
  ,app_version VARCHAR
  ,app_bu VARCHAR
  ,app_range VARCHAR
)with(
   type='testsink'
);

insert into tt_output
select
  md5_app_id
  ,app_version
  ,app_bu
  ,app_range
from union_view
where app_version is not null;
