from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat
from pyspark.sql import types as T

catalog = "main__build"
schema = dbName = db = "dbdemos_fsi_smart_claims"
volume_name = "volume_claims"

def flatten_struct(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StructType):
            for child in field.dataType:
                df = df.withColumn(field.name + '_' + child.name, F.col(field.name + '.' + child.name))
            df = df.drop(field.name)
    return df
