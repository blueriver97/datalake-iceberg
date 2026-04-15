-- ============================================================================
-- V2 명명 규칙 마이그레이션 (2026-04-15)
-- ============================================================================
-- 변경 내역
--   1) ops_bronze    → di_ops      (운영 메타 schema)
--   2) store_bronze  → local_store  (로컬 테스트 데이터 schema)
--   3) di_ops 두 워터마크 테이블의 컬럼 bronze_schema → iceberg_schema
--   4) 워터마크 데이터의 schema 값 갱신 (V1 → V2)
--
-- 실행 전제
--   - Spark SQL on Iceberg (Glue Catalog)
--   - 모든 적재/유지보수 파이프라인이 중지된 상태에서 실행
--   - awsdatacatalog가 default catalog로 설정되어 있다고 가정
--   - <bucket>, <data_path>는 환경에 맞게 치환
--
-- 실행 방법: spark-sql 또는 spark.sql() 한 문장씩 실행
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. 새 데이터베이스 생성
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS di_ops
  LOCATION 's3a://<bucket><data_path>/di_ops';

CREATE DATABASE IF NOT EXISTS local_store
  LOCATION 's3a://<bucket><data_path>/local_store';

-- ----------------------------------------------------------------------------
-- 2. ops_bronze → di_ops 테이블 RENAME
--    (Glue catalog metadata만 갱신; S3 데이터 파일은 원래 위치에 남음)
-- ----------------------------------------------------------------------------
ALTER TABLE ops_bronze.cdc_watermark         RENAME TO di_ops.cdc_watermark;
ALTER TABLE ops_bronze.maintenance_watermark RENAME TO di_ops.maintenance_watermark;

-- ----------------------------------------------------------------------------
-- 3. di_ops 두 테이블의 컬럼 RENAME (Iceberg format-version 2 지원)
-- ----------------------------------------------------------------------------
ALTER TABLE di_ops.cdc_watermark         RENAME COLUMN bronze_schema TO iceberg_schema;
ALTER TABLE di_ops.maintenance_watermark RENAME COLUMN bronze_schema TO iceberg_schema;

-- ----------------------------------------------------------------------------
-- 4. 워터마크 데이터의 schema 컬럼 값 갱신 (V1 → V2 매핑)
--    환경에 다른 V1 schema가 더 있다면 동일 패턴으로 UPDATE 추가
--    예: SELECT DISTINCT iceberg_schema FROM di_ops.cdc_watermark; 로 확인
-- ----------------------------------------------------------------------------
UPDATE di_ops.cdc_watermark
   SET iceberg_schema = 'local_store'
 WHERE iceberg_schema = 'store_bronze';

UPDATE di_ops.maintenance_watermark
   SET iceberg_schema = 'local_store'
 WHERE iceberg_schema = 'store_bronze';

-- ----------------------------------------------------------------------------
-- 5. store_bronze → local_store 테이블 RENAME
--    실제 환경의 테이블 목록에 맞게 추가/제거 (SHOW TABLES IN store_bronze)
-- ----------------------------------------------------------------------------
ALTER TABLE store_bronze.tb_lower         RENAME TO local_store.tb_lower;
ALTER TABLE store_bronze.TB_UPPER         RENAME TO local_store.TB_UPPER;
ALTER TABLE store_bronze.TB_COMPOSITE_KEY RENAME TO local_store.TB_COMPOSITE_KEY;

-- ----------------------------------------------------------------------------
-- 6. 빈 데이터베이스 DROP
-- ----------------------------------------------------------------------------
DROP DATABASE ops_bronze;
DROP DATABASE store_bronze;

-- ============================================================================
-- 참고 1: S3 경로 정합화 (선택 사항)
-- ============================================================================
-- ALTER TABLE RENAME은 catalog metadata만 갱신하므로, 데이터 파일은 여전히
--   s3a://<bucket><data_path>/store_bronze/<table>/   (이전 경로)
-- 위치에 남는다. S3 경로까지 정책 V2와 일치시키려면 CTAS로 새 경로에 다시
-- 쓰고 구 테이블을 DROP한다. snapshot history는 손실된다.
--
-- 예시:
--   CREATE TABLE local_store.tb_lower
--     USING iceberg
--     LOCATION 's3a://<bucket><data_path>/local_store/tb_lower'
--     TBLPROPERTIES ('format-version' = '2')
--     AS SELECT * FROM store_bronze.tb_lower;
--   DROP TABLE store_bronze.tb_lower PURGE;

-- ============================================================================
-- 참고 2: 체크포인트 / 시그널 경로
-- ============================================================================
-- 코드의 체크포인트 경로는 토픽 단위(s3a://<bucket>/iceberg/checkpoint/<dag>/<topic>)
-- 이고 시그널 경로는 dag-id 단위라 schema 변경 영향이 없다. 그대로 유지.

-- ============================================================================
-- 참고 3: 롤백 시나리오
-- ============================================================================
-- 위 5번까지 진행 후 6번 DROP 전까지는 다음으로 되돌릴 수 있다.
--
--   ALTER TABLE local_store.<table>           RENAME TO store_bronze.<table>;
--   ALTER TABLE di_ops.cdc_watermark          RENAME COLUMN iceberg_schema TO bronze_schema;
--   ALTER TABLE di_ops.maintenance_watermark  RENAME COLUMN iceberg_schema TO bronze_schema;
--   ALTER TABLE di_ops.cdc_watermark          RENAME TO ops_bronze.cdc_watermark;
--   ALTER TABLE di_ops.maintenance_watermark  RENAME TO ops_bronze.maintenance_watermark;
--   DROP DATABASE local_store;
--   DROP DATABASE di_ops;
--
-- 6번 DROP DATABASE 이후의 롤백은 새 데이터베이스 재생성부터 다시 진행.
