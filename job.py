from datetime import timedelta, datetime
from extractors.g1 import extract_g1_data, load_g1_raw_data
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule


@task(max_retries=1, retry_delay=timedelta(seconds=1))
def transform(data):
    print("transformed")
    return "transformed"


@task(max_retries=1, retry_delay=timedelta(seconds=1))
def load_final_dataset(data):
    print("dataset loaded")


def main():

    # Scheduled to run every 24 hours
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(hours=24),
    )

    # Main project flow
    with Flow("Fake News Data Extractor 2000", schedule=schedule) as flow:

        # Extract
        g1_url = "https://g1.globo.com/fato-ou-fake/"
        g1_raw_data = extract_g1_data(g1_url)

        # Transform
        fake_news_dataset = transform(g1_raw_data)

        # --- Load ---

        # ----- Load Raws -----
        load_g1_raw_data(g1_raw_data)

        # ----- Load Transformations -----

        # ----- Load Final Dataset -----
        load_final_dataset(fake_news_dataset)

    flow.run()


if __name__ == "__main__":
    main()
