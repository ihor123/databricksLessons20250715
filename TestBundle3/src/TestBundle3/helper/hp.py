from pyspark.sql.functions import current_timestamp, lit, year, month, dayofmonth, round, rand, col, from_unixtime
from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, BooleanType, StructType, StructField, DoubleType, LongType
from pyspark.sql import SparkSession, DataFrame

from typing import Optional

class hp:
    catalogue = "workspace"
    database = "default"
    display_target = "development"
    spark : Optional[SparkSession] = None

    def setup(self, catalogue: Optional[str] = "workspace", database: Optional[str] = "default", display_target: Optional[str] = "development"):
        s = None
        try:
            s = dbutils.widgets.get('catalogue_bronze_name')
        except:
            print('No catalogue_bronze_name parameter was found')
            pass
        if s is not None:
            self.catalogue = s
        s = None
        try:
            s = dbutils.widgets.get('database_bronze_name')
        except:
            print('No database_bronze_name parameter was found')
            pass
        if s is not None:
            self.database = s
        s = None
        try:
            s = dbutils.widgets.get('display_target')
        except:
            print('No display_target parameter was found')
            pass
        if s is not None:
            self.display_target = s
        s = None


        if catalogue is not None:
            self.catalogue = catalogue
        if database is not None:
            self.database = database
        if display_target is not None:
            self.display_target = display_target


        self.spark.sql(f'USE CATALOG {self.catalogue}')
        self.spark.sql(f'USE DATABASE {self.database}')


        #spark.sql("DROP VARIABLE IF EXISTS catalogue_bronze_name")
        #spark.sql("DROP VARIABLE IF EXISTS database_bronze_name")
        #spark.sql("DROP VARIABLE IF EXISTS display_target")
        #spark.sql("DECLARE VARIABLE catalogue_bronze_name STRING")
        #spark.sql("DECLARE VARIABLE database_bronze_name STRING")
        #spark.sql("DECLARE VARIABLE display_target STRING")
        
        self.spark.sql("drop temporary variable if exists catalogue_bronze_name;")
        self.spark.sql("declare variable catalogue_bronze_name string;")
        self.spark.sql(f"set variable catalogue_bronze_name='{self.catalogue}';")          

        self.spark.sql("drop temporary variable if exists database_bronze_name;")
        self.spark.sql("declare variable database_bronze_name string;")
        self.spark.sql(f"set variable database_bronze_name='{self.database}';")          

        self.spark.sql("drop temporary variable if exists display_target;")
        self.spark.sql("declare variable display_target string;")
        self.spark.sql(f"set variable display_target='{self.display_target}';")   

    def __init__(self, spark, catalogue: Optional[str] = None, database: Optional[str] = None, display_target: Optional[str] = None):

        self.spark = spark
        #self.setup(catalogue = catalogue, database = database, display_target = display_target)


    def add_standard_columns(self, df, createdBy: Optional[str] = None, modifiedBy: Optional[str] = None):
        df = df.withColumn('timestamp', current_timestamp())

        if modifiedBy is not None:
            df = df.withColumn('modifiedOn', current_timestamp().cast(TimestampType()))
            df = df.withColumn('modifiedBy', lit(modifiedBy).cast(StringType()))
        else:
            df = df.withColumn('modifiedOn', lit(None).cast(TimestampType()))
            df = df.withColumn('modifiedBy', lit(None).cast(StringType()))
        if createdBy is not None:
            df = df.withColumn('createdOn', current_timestamp())
            df = df.withColumn('createdBy', lit(createdBy).cast (StringType()))
        else:
            df = df.withColumn('createdOn', lit(None).cast(TimestampType()))
            df = df.withColumn('createdBy', lit(None).cast (StringType()))
        
        df = df.withColumn('isCurrent', lit(True).cast(BooleanType()))

        return df
    
    def transactions_pt(self, df):
        df = (
          df
            .withColumn('year', year(df['time']))
            .withColumn('month', month(df['time']))
            .withColumn('customer_partition', df['customer_id']%10)
        )

        return df


