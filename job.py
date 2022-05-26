from extractors.g1 import extract_g1
from extractors.efarsas import extract_efarsas_data
from datetime import timedelta, datetime

# Prefect imports
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

# PySpark imports
from pyspark.sql import functions as F
from pyspark.sql import Window as Window
from pyspark.sql import types
from pyspark.context import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


# Extractors imports
# TO DO: Remove this function from here, it should have it's own file
@task(max_retries=1, retry_delay=timedelta(seconds=1))
def load_raw_data(raw_data_df, publisher_name):
    #raw_data_df.write.option("header", True).option("delimiter", ",").csv(
    #    "data/raw/"+publisher_name+"/")
    raw_data_df.write.saveAsTable(
        name="data/raw/"+publisher_name+".json",
        format="json",
        mode="append"
    )


def main():
    # Scheduled to run every 24 hours
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(hours=24),
    )

    # Main project flow
    with Flow("Fact Check Extractor ETL", schedule=schedule) as flow:

        # Extract
        g1_url = "https://g1.globo.com/fato-ou-fake/"
        g1_raw_data_df = extract_g1(g1_url)

        # ----- Load Raws -----
        load_raw_data(g1_raw_data_df, "g1")

    # Executes the flow
    flow.run()


if __name__ == "__main__":
    main()
