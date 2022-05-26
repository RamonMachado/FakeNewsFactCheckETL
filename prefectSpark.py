from pyspark.sql import functions as F
from pyspark.sql import Window as Window
from pyspark.sql import types
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import matplotlib.pyplot as plt
from prefect import task, Flow

sc = SparkContext('local')
spark = SparkSession(sc)


@task
def extract_grades_data():
    # reading csv file
    history_df = spark.read.csv(
        "data/historico_notas.utf8.csv", header=True, sep=",")
    # converting Media Final from String to Double
    history_df = history_df.withColumn(
        "Media Final", history_df["Media Final"].cast(types.DoubleType()))

    return history_df


@task
def transform(history_df):
    # finding the greatest grade for each semester
    greatest_grade_semester_df = history_df.withColumn("rank", F.rank().over(Window.partitionBy(
        "Período").orderBy(F.desc("Media Final")))).select("Período", "Disciplina", "Media Final")
    greatest_grade_semester_df = greatest_grade_semester_df.filter(
        F.col("rank") == 1).orderBy(F.col("Período"))

    return greatest_grade_semester_df


@task
def load_greatest_grades(greatest_grades_df):
    greatest_grades_df.write.option("header", True).option("delimiter", ",").csv(
        "data/maiores_notas")


def main():
    with Flow("etl") as flow:
        grades_df = extract_grades_data()
        greatest_grades_by_semester = transform(grades_df)
        load_greatest_grades(greatest_grades_by_semester)
    flow.run()


if __name__ == "__main__":
    main()
