from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, DoubleType
from pyspark import SparkFiles
import zipfile
from os import mkdir, getcwd


def init_spark():
    """
    Function that create the session in Spark
    :output (object): spark session
    :output (object): spark context
    """
    spark = SparkSession.builder.appName("Adevinta_Challenge").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def get_schema():
    """
    Function that returns the schema for the csv input data
    :output (object): schema
    """
    schema = StructType([ \
        StructField("platform",StringType(),True), \
        StructField("date",DateType(),True), \
        StructField("channel_name",StringType(),True), \
        StructField("bidder",LongType(),True), \
        StructField("buyer",LongType(),True), \
        StructField("device_type_name",StringType(),True), \
        StructField("format_name",StringType(),True), \
        StructField("transaction_type_name",StringType(),True), \
        StructField("revenue_resold",DoubleType(),True), \
        StructField("impressions_resold",DoubleType(),True), \
        StructField("clicks",DoubleType(),True) \
      ])
    return schema


def replace_values(df, list_with_replace_lists, list_names, column_name):
    """
    Function that replace the values of the column specify
    :input df(object): dataframe with data
    :input list_with_replace_lists(list(list)): list of list with values to be replaced
    :input list_names(list): list with values to use for replace
    :input column_name(string): column name to replace
    :output (object): dataframe with column values replaced
    """
    for list_with_replace_values, name in zip(list_with_replace_lists, list_names):
        df = df.withColumn(column_name, when(df[column_name].isin(list_with_replace_values), name).otherwise(df[column_name]))
    df = df.withColumn(column_name, when(df[column_name].isin(list_names), df[column_name]).otherwise("Unknown"))
    return df


def replace_device(df):
    """
    Function that replace the values for the device column
    :input df(object): dataframe with data
    :output (object): dataframe with column device_type_name replaced
    """
    list_mobile = ["Mobile","Other-Android-web","Other-iOS-app","Other-iOS-web","Smartphone-Android","Smartphone-Other","Smartphone-Windows"]
    list_desktop = ["Connected TV","Desktop & laptop","Desktop-Mac OS","Desktop-Other","Desktop-Unknown","Desktop-Windows","Other-Mac OS-web"]
    list_tablet = ["Tablet","Tablet-Android-app","Tablet-Android-web","Tablet-Other-web","Tablet-Windows-web"]
    list_all = [list_mobile,list_desktop,list_tablet]
    list_names = ["Mobile","Desktop","Tablet"]
    df = replace_values(df,list_all,list_names,"device_type_name")
    return df


def replace_channel(df):
    """
    Function that replace the values for the channel column
    :input df(object): dataframe with data
    :output (object): dataframe with column channel_name replaced
    """
    list_programmatic = ["Always-on deal - Improve","Programmatic","RTB - Improve","Universal deal - Improve"]
    list_direct = ["Direct","direct-null"]
    list_all = [list_programmatic,list_direct]
    list_names = ["Programmatic","Direct"]
    df = replace_values(df,list_all,list_names,"channel_name")
    return df


def get_olap_cube(df):
    """
    Function that creates an olap cube from the dataframe
    :input df(object): dataframe with data
    :output (object): dataframe with olap cube
    """
    name_list = ["Date","Country","Device","Channel"]
    for i in range(len(name_list)):
        group_list = name_list[:i+1]
        df_temp = df.groupBy(group_list).agg(sum("Revenue").alias("Revenue"),avg("CTR").alias("CTR"))
        temp_list = [x for x in name_list if x not in group_list]
        for element in temp_list:
            df_temp = df_temp.withColumn(element,lit("ALL"))    
        df_temp = df_temp.select("Date","Country","Device","Channel","Revenue","CTR")
        if i == 0:
            df_final = df_temp
        else:
            df_final = df_final.union(df_temp)
    return df_final


def unzip_and_get_file_paths():
    """
    Function that unzip the file input.zip and return the path to the .csv files
    :output (list): list with local paths to unzipped files
    :output (string): local path with unzipped files
    """
    folder_to_unzip = "descompress" 
    mkdir("./"+folder_to_unzip)
    file_path_temp = getcwd()+"/"+folder_to_unzip+"/"
    list_paths = []
    with zipfile.ZipFile("./input.zip","r") as zip_ref:
        for subfile in zip_ref.namelist():
            if ".csv" in subfile:
                zip_ref.extract(subfile,"./"+folder_to_unzip)
                list_paths.append(file_path_temp+subfile)
    file_path_temp = file_path_temp+"input/"
    return list_paths, file_path_temp


def read_data_and_concat(spark, spark_context, files_path, list_paths):
    """
    Function that read all the unzipped .csv files, add the respective country and concatenate all of then in a single dataframe
    :input spark(object): spark session
    :input spark_context(object): spark context
    :input files_path(string): local path with unzipped files
    :input list_paths(list): list with local paths to unzipped files    
    :output (object): dataframe
    """
    spark_context.addFile(files_path,True)
    schema = get_schema()
    i = 0
    for file_path in list_paths:
        df_temp = spark.read.option("header", "true").option("delimiter",";").schema(schema).load(SparkFiles.get(file_path),format="csv")
        df_temp = df_temp.withColumn("country",lit(file_path.split("/")[-2]))
        if i == 0:
            df = df_temp
        else:
            df = df.union(df_temp)
        i  = i + 1
    return df


def main():
    spark, sc = init_spark()
    list_paths, files_path = unzip_and_get_file_paths()
    df = read_data_and_concat(spark,sc,files_path,list_paths)
    df = df.distinct()
    df = df.na.drop()
    df = replace_device(df)
    df = replace_channel(df)
    df = df.withColumn("CTR",(col("clicks")/col("impressions_resold"))*100)
    df = df.selectExpr("date as Date","country as Country","device_type_name as Device","channel_name as Channel","revenue_resold as Revenue","CTR")
    res = get_olap_cube(df)
    res.write.parquet("output")


if __name__ == "__main__":
    main()
