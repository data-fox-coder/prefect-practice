## boilerplate .py code

@task(retries=3, retry_delay_seconds=15)
def extract(api_client):
    response = requests.get(url)
    response.raise_for_status()  # bad status → exception → retry
    return response.json()

@task
def transform(raw):
    return [reshape(r) for r in raw]

@flow
def run_pipeline():
    data = extract(client)
    clean = transform(data)   # waits for extract automatically