
CREATE TABLE wdm_shuyu_stream(
    client_ip varchar,
    protocol_version varchar,
    imei varchar,
    imsi varchar,
    brand varchar,
    cpu_1 varchar,
    device_id varchar,
    device_model varchar,
    resolution varchar,
    carrier varchar,
    access varchar,
    access_subtype varchar,
    channel varchar,
    app_key varchar,
    app_version varchar,
    long_login_nick varchar,
    user_nick varchar,
    phone_number varchar,
    country varchar,
    lang varchar,
    os varchar,
    os_version varchar,
    sdk_type varchar,
    sdk_version varchar,
    imeisi varchar,
    utdid varchar,
    reserve1 varchar,
    reserve2 varchar,
    reserve3 varchar,
    reserves varchar,
    local_time varchar,
    local_timestamp varchar,
    pg varchar,
    event_id varchar,
    arg1 varchar,
    arg2 varchar,
    arg3 varchar,
    args varchar,
    dy varchar,
    hour_id varchar,
    server_timestamp varchar,
    seq varchar,
    ptime as PROCTIME()
)with(
  type='testsource'
);

CREATE TABLE mv_music_input
(
    mv_id           bigint,
    title           varchar,
    mv_category     int,
    gmt_publish      bigint,
    gmt_upload       bigint,
    is_play          bigint,
    is_deleted       bigint,
    PRIMARY KEY(mv_id),
    PERIOD FOR SYSTEM_TIME
) with(
    type='csv',
    path='xxxx'
);


CREATE TABLE mv_rank_input(
    gmt_create TIMESTAMP,
    gmt_modified TIMESTAMP,
    mv_id BIGINT,
    score BIGINT,
    `rank` BIGINT,
    `type` INT,
    detail VARCHAR,
    PRIMARY KEY (`mv_id`),
    PERIOD FOR SYSTEM_TIME
) with(
    type='csv',
    path='xxxx'
);

CREATE VIEW mv_1d_playct AS
SELECT
    mv_id
    , HOP_START(ptime, INTERVAL '5' MINUTE, INTERVAL '1' day) AS start_time
    , HOP_END(ptime, INTERVAL '5' MINUTE, INTERVAL '1' day) AS end_time
    , COUNT(imeisi) AS play_ct
FROM (
    SELECT
        imeisi,
        ptime,
        regexp_extract(args, 'prev_songid=([^,]+)', 1) AS mv_id,
        regexp_extract(args, 'timestampa=([^,]+)', 1) AS action_timestamp,
        regexp_extract(args, 'prev_playtime=([^,]+)', 1) AS playtime,
        regexp_extract(args, 'prev_fulltime=([^,]+)', 1) AS fulltime
    FROM
        wdm_shuyu_stream
    WHERE event_id = '19999' and arg1 = 'playsong' and regexp_extract(args, 'file=([^,]+)', 1)='mv'
) a
WHERE playtime>90000 or playtime=fulltime
GROUP BY HOP(ptime, INTERVAL '5' MINUTE, INTERVAL '1' day), mv_id
;


CREATE VIEW mv_1d_playct_filtered AS
SELECT
    a.mv_id,start_time,end_time,play_ct,mv_category,title
FROM (
    SELECT
        play_ct AS mv_id, start_time, end_time, play_ct
    FROM
        mv_1d_playct
) a
JOIN
(
    SELECT
        mv_id, mv_category, title
    FROM
        mv_music_input
    FOR SYSTEM_TIME AS OF PROCTIME()
    WHERE
        (gmt_publish >= NOW(-91*86400) OR gmt_upload >=NOW(-31*86400))
        and is_play = 1
        and is_deleted=0
        and mv_category in(1,2,3)
        and mv_id>0
) b
ON a.mv_id = b.mv_id
;

CREATE TABLE mv_rank_realtime(
    gmt_create TIMESTAMP,
    gmt_modified TIMESTAMP,
    mv_id BIGINT,
    score BIGINT,
    `rank` BIGINT,
    `type` INT,
    detail VARCHAR,
    PRIMARY KEY (`mv_id`),
    PERIOD FOR SYSTEM_TIME
) with(
    type='csv',
    path='xxxx'
);

CREATE TABLE mv_1d_output_real(
    gmt_create TIMESTAMP,
    gmt_modified TIMESTAMP,
    mv_id BIGINT,
    score BIGINT,
    `rank` INT,
    attribute VARCHAR,
    PRIMARY KEY (`rank`)
) with(
    type='testsink',
    ignoreWrite='true'
);

INSERT INTO mv_1d_output_real
SELECT
    start_time AS gmt_create,
    end_time AS gmt_modified,
    a.mv_id ,
    play_ct AS score,
    CAST(rk AS INT) AS `rank`,
    CONCAT('changes:',CAST((COALESCE(c.`rank`, 101)-a.rk) AS VARCHAR)) AS attribute
FROM
(
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY end_time DESC, play_ct DESC) AS rk
    FROM
        mv_1d_playct_filtered
) a
LEFT OUTER JOIN
(
    SELECT
        mv_id, `rank`
    FROM
        mv_rank_realtime
    FOR SYSTEM_TIME AS OF PROCTIME()
) c
ON a.mv_id = c.mv_id
WHERE rk <= 100;


