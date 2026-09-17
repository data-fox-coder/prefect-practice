import requests

BASE_URL = "https://api.cqc.org.uk/public/v1"
PARTNER_CODE = "data-fox-coder-api-checker"

response = requests.get(
    f"{BASE_URL}/providers",
    params={"partnerCode": PARTNER_CODE},
)
response.raise_for_status()
providers = response.json()