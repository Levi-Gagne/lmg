# utils/spark_schema_helper.py

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    TimestampType, DateType, ArrayType, MapType, BinaryType, ByteType, DecimalType
)

SPARK_TYPE_MAP = {
    "string": StringType(),
    "str": StringType(),
    "varchar": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "bigint": LongType(),
    "long": LongType(),
    "smallint": IntegerType(),
    "short": IntegerType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "decimal": DecimalType(38, 18),
    "date": DateType(),
    "timestamp": TimestampType(),
    "array": ArrayType(StringType()),
    "map": MapType(StringType(), StringType()),
    "binary": BinaryType(),
    "byte": ByteType(),
}

def get_spark_struct_fields(columns: list) -> list:
    fields = []
    for col in columns:
        col_type = col.get("datatype", "").lower()
        spark_type = SPARK_TYPE_MAP.get(col_type)
        if not spark_type:
            raise ValueError(f"Unsupported catalog datatype '{col_type}' in column '{col.get('name', '')}'.")
        fields.append(
            StructField(
                col.get("name", ""),
                spark_type,
                col.get("nullable", True)
            )
        )
    return fields

def get_spark_schema(columns: list) -> StructType:
    return StructType(get_spark_struct_fields(columns))