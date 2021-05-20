import unittest
from os import getcwd
from pyspark import SparkFiles
from pyspark.sql.functions import lit
from main import init_spark, get_schema, process_logic


class pysparkTest(unittest.TestCase):
    """
    Test Adevinta Pyspark Challenge
    """
    def test_transform_data(self):
        """
        Function that checks all the main process functionality based on check:
        :num rows
        :num of columns
        :column names
        """
        spark, sc = init_spark()
        file_input_path = getcwd()+"/test_data.csv"
        file_compare_path = getcwd()+"/output_test"

        sc.addFile(file_input_path,True)
        df = spark.read.option("header", "true").option("delimiter",";").schema(get_schema()).load(SparkFiles.get(file_input_path),format="csv")
        df = df.withColumn("country",lit("country_test"))
        df = process_logic(df)

        sc.addFile(file_compare_path,True)
        df_compare = spark.read.parquet(SparkFiles.get(file_compare_path))

        expected_cols = len(df_compare.columns)
        expected_rows = df_compare.count()
        cols = len(df.columns)
        rows = df.count()
        
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in df.columns
                         for col in df_compare.columns])


if __name__ == '__main__':
    unittest.main()
