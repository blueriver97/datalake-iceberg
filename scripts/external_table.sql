CREATE
EXTERNAL TABLE IF NOT EXISTS uxlog_bronze.tbl_uxlog (
  id               string,
  timestamp        string,
  appversion       string,
  platform         string,
  page             string,
  slot             string,
  action           string,
  uuid             string,
  membership       string,
  sessionid        string,
  cid              string,
  ecid             string,
  clientip         string,
  country          string,
  city             string,
  language         string,
  hostname         string,
  os               string,
  regtime          string,
  useragent        string,
  browser          string,
  resolution       string,
  pageurl          string,
  referrer         string,
  utm struct<
    source: string,
    medium: string,
    campaign: string,
    content: string,
    term: string
  >,
  extend struct<
    index: string,
    name: string,
    type: string,
    seq: string,
    groupid: string,
    custom: array<string>,
    currenttime: string,
    status: string,
    keyword: string,
    collections: array<struct<
      name: string,
      keyword: string,
      total: string,
      count: string,
      page: string,
      size: string
    >>
  >
)
PARTITIONED BY (
  `service` string,
  `year` string,
  `month` string,
  `day` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
-- ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://hunet-di-data-lake-prod/data/log/ux_log'
TBLPROPERTIES (
  'parquet.mergerSchema'='true',
  'projection.enabled'='true',
--  'projection.service.type'='enum',
--  'projection.service.values'='CEO,LABS,CUSTOM,LEARNINGMAKER,LabsCustomTrainingCenter',
  'projection.year.type'='date',
  'projection.year.range'='2026,NOW',
  'projection.year.format'='yyyy',
  'projection.month.type'='integer',
  'projection.month.range'='1,12',
  'projection.month.digits'='2',
  'projection.day.type'='integer',
  'projection.day.range'='1,31',
  'projection.day.digits'='2',
  'storage.location.template'='s3://blueriver-datalake/data/log/ux_log/service=${service}/year=${year}/month=${month}/day=${day}/'
)
