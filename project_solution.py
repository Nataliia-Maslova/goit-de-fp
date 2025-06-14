from prefect import flow
from landing_to_bronze import landing_to_bronze
from bronze_to_silver import bronze_to_silver
from silver_to_gold import silver_to_gold
from prefect.settings import PREFECT_API_URL
from prefect.settings import temporary_settings

with temporary_settings({PREFECT_API_URL: None}):
    @flow
    def etl_pipeline():
        for table in ["athlete_bio", "athlete_event_results"]:
            landing_to_bronze(table)
        bronze_to_silver()
        silver_to_gold()

    if __name__ == "__main__":
        etl_pipeline()