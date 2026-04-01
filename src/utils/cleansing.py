"""
데이터 정제 유틸리티
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame


def trim_string_columns(df: DataFrame) -> DataFrame:
    """StringType 컬럼의 앞뒤 공백을 제거한다.

    JDBC로 읽은 CHAR 타입 컬럼에 패딩된 공백을 정리하는 데 사용한다.
    """
    return df.select(
        [
            F.trim(F.col(field.name)).alias(field.name)
            if isinstance(field.dataType, T.StringType)
            else F.col(field.name).alias(field.name)
            for field in df.schema.fields
        ]
    )
