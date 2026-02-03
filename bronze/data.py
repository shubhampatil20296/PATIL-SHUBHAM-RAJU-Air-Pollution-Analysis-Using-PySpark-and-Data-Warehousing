import os
import json
import time
import requests
from dotenv import load_dotenv

# -------------------------
# 1. Load environment variables
# -------------------------
load_dotenv()  # loads .env from project root

API_TOKEN = os.getenv("AQI_API_TOKEN")
BASE_URL = os.getenv("AQI_BASE_URL", "https://api.waqi.info")

if not API_TOKEN:
    raise ValueError("AQI_API_TOKEN not found in .env file")

# -------------------------
# 2. Configuration
# -------------------------
CITIES = [
    "delhi",
    "mumbai",
    "bengaluru",
    "chennai",
    "kolkata",
    "hyderabad",
    "pune",
    "ahmedabad",
    "jaipur",
    "lucknow",
    "kanpur",
    "nagpur",
    "indore",
    "bhopal",
    "patna",
    "ranchi",
    "raipur",
    "bhubaneswar",
    "visakhapatnam",
    "vijayawada",
    "tirupati",
    "kochi",
    "trivandrum",
    "coimbatore",
    "madurai",
    "salem",
    "trichy",
    "erode",
    "tiruppur",
    "trivandrum",
    "surat",
    "vadodara",
    "rajkot",
    "gandhinagar",
    "udaipur",
    "jodhpur",
    "ajmer",
    "amritsar",
    "ludhiana",
    "jalandhar",
    "chandigarh",
    "panipat",
    "sonipat",
    "gurgaon",
    "faridabad",
    "noida",
    "greater noida",
    "ghaziabad",
    "meerut",
    "bareilly",
    "agra",
    "mathura",
    "aligarh",
    "varanasi",
    "prayagraj",
    "gorakhpur",
    "gaya",
    "muzaffarpur",
    "darbhanga",
    "siliguri",
    "asansol",
    "durgapur",
    "howrah",
    "kharagpur",
    "jamshedpur",
    "bokaro",
    "dhanbad",
    "dehradun",
    "haridwar",
    "rishikesh",
    "shimla",
    "manali",
    "srinagar",
    "jammu"
]

OUTPUT_PATH = "bronze/raw_aqi_data.json"
REQUEST_TIMEOUT = 10      # seconds
MAX_RETRIES = 3           # retry per city
SLEEP_BETWEEN_CALLS = 1   # be polite to API

# -------------------------
# 3. Fetch AQI data (RAW)
# -------------------------
all_data = []

for city in CITIES:
    url = f"{BASE_URL}/feed/{city}/?token={API_TOKEN}"
    print(f"Fetching AQI data for {city}...")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()

            all_data.append({
                "city": city,
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "response": data
            })

            break  # success → exit retry loop

        except requests.exceptions.RequestException as e:
            print(f"[WARN] Attempt {attempt} failed for {city}: {e}")

            if attempt == MAX_RETRIES:
                print(f"[ERROR] Skipping {city} after {MAX_RETRIES} failures")
                all_data.append({
                    "city": city,
                    "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "response": None,
                    "error": str(e)
                })

        time.sleep(SLEEP_BETWEEN_CALLS)

# -------------------------
# 4. Write RAW data to Bronze layer
# -------------------------
os.makedirs("bronze", exist_ok=True)

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(all_data, f, indent=2, ensure_ascii=False)

print(f"\n✅ Raw AQI data written successfully to: {OUTPUT_PATH}")
