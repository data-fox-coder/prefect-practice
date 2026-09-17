from prefect import flow, task, get_run_logger
import time


@task(
    retries=3,
    retry_delay_seconds=2,
    name="Extract Dataset"
)
def extract_data(raw_value: int):
    logger = get_run_logger()
    logger.info(f"Extracting dataset starting with base value: {raw_value}")
    
    time.sleep(1)

    # Trigger a failure on initial attempts with negative values
    if raw_value < 0:
        logger.warning(f"Negative input detected ({raw_value})! Raising error to test retries...")
        raise ValueError("Raw value cannot be negative!")

    return [raw_value * i for i in range(1, 5)]


@task(
    retries=2,
    retry_delay_seconds=1,
    name="Transform Dataset"
)
def transform_data(data: list[int], multiplier: int):
    logger = get_run_logger()
    logger.info(f"Applying multiplier {multiplier} to input data: {data}")
    return [x * multiplier for x in data]


@flow(name="retry_pipeline_flow")
def my_pipeline(start_number: int = 10, scale_factor: int = 2):
    logger = get_run_logger()
    logger.info("Pipeline execution initialized.")

    raw_data = extract_data(raw_value=start_number)
    final_data = transform_data(data=raw_data, multiplier=scale_factor)

    logger.info("Pipeline finished successfully.")
    return final_data


if __name__ == "__main__":
    print("\n--- Triggering Retries with Negative Parameter ---")
    try:
        # Passing a negative number causes the task to fail and retry
        result = my_pipeline(start_number=-5, scale_factor=2)
        print(f"Final Result: {result}")
    except Exception as e:
        print(f"\nFlow failed after exhausting all retries: {e}")


## boilerplate .py code for connecting to APIs and transforming data

# @task(retries=3, retry_delay_seconds=15)
# def extract(api_client):
#     response = requests.get(url)
#     response.raise_for_status()  # bad status → exception → retry
#     return response.json()

# @task
# def transform(raw):
#     return [reshape(r) for r in raw]

# @flow
# def run_pipeline():
#     data = extract(client)
#     clean = transform(data)   # waits for extract automatically