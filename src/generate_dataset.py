"""
generate_dataset.py
--------------------
Generates a realistic, intentionally messy retail sales transaction dataset
and writes it to data/raw/retail_sales_raw.csv.

The dataset is deliberately seeded with the kinds of data-quality problems
a real retail transactional system produces:
    - missing customer_id / payment_method / category / city values
    - negative quantities (return / entry errors)
    - malformed / impossible order_date values
    - delivery_date earlier than order_date
    - total_amount that doesn't reconcile with quantity * unit_price * (1 - discount)
    - inconsistent category casing/spacing and legacy city name aliases
    - fully duplicated transaction rows

This is what makes the downstream cleaning, transformation and validation
stages of the pipeline meaningful instead of cosmetic.

Run:
    python src/generate_dataset.py
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SEED = 42
N_BASE_ROWS = 99_000          # unique base transactions
N_DUPLICATES = 1_000          # exact duplicate rows appended on top
TOTAL_ROWS = N_BASE_ROWS + N_DUPLICATES  # -> 100,000

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "raw", "retail_sales_raw.csv"
)

random.seed(SEED)
np.random.seed(SEED)

# ---------------------------------------------------------------------------
# Reference / dimension data
# ---------------------------------------------------------------------------
N_CUSTOMERS = 15_000
CUSTOMER_IDS = [f"CUST{str(i).zfill(5)}" for i in range(1, N_CUSTOMERS + 1)]

# A handful of "loyal" customers receive a higher selection weight so the
# customer-level KPIs (top customers, repeat purchase behaviour) look realistic.
CUSTOMER_WEIGHTS = np.random.pareto(a=2.5, size=N_CUSTOMERS) + 0.1
CUSTOMER_WEIGHTS = CUSTOMER_WEIGHTS / CUSTOMER_WEIGHTS.sum()

# category -> (min_price, max_price)
CATEGORY_PRICE_RANGE = {
    "Electronics": (1500, 60000),
    "Clothing": (300, 4000),
    "Footwear": (500, 6000),
    "Grocery": (20, 1200),
    "Furniture": (2000, 45000),
    "Beauty & Personal Care": (100, 3000),
    "Sports & Fitness": (300, 15000),
    "Books & Stationery": (50, 1500),
    "Home Decor": (200, 8000),
    "Toys & Games": (150, 5000),
}
CATEGORIES = list(CATEGORY_PRICE_RANGE.keys())

# Build a fixed product catalogue: ~60 products per category
PRODUCTS = []  # list of (product_id, category, base_price)
pid_counter = 1
for category, (lo, hi) in CATEGORY_PRICE_RANGE.items():
    for _ in range(60):
        base_price = round(np.random.uniform(lo, hi), 2)
        PRODUCTS.append((f"PROD{str(pid_counter).zfill(4)}", category, base_price))
        pid_counter += 1

# city -> state
CITY_STATE = {
    "Mumbai": "Maharashtra",
    "Pune": "Maharashtra",
    "Nagpur": "Maharashtra",
    "Delhi": "Delhi",
    "Bangalore": "Karnataka",
    "Hyderabad": "Telangana",
    "Chennai": "Tamil Nadu",
    "Kolkata": "West Bengal",
    "Ahmedabad": "Gujarat",
    "Surat": "Gujarat",
    "Vadodara": "Gujarat",
    "Jaipur": "Rajasthan",
    "Lucknow": "Uttar Pradesh",
    "Kanpur": "Uttar Pradesh",
    "Agra": "Uttar Pradesh",
    "Indore": "Madhya Pradesh",
    "Bhopal": "Madhya Pradesh",
    "Patna": "Bihar",
    "Guwahati": "Assam",
    "Agartala": "Tripura",
}
CITIES = list(CITY_STATE.keys())

# Legacy / inconsistent aliases used to dirty up the city column on purpose.
# The transformation stage is responsible for mapping these back to a single
# canonical city name.
CITY_ALIASES = {
    "Mumbai": ["mumbai", "MUMBAI", "Bombay", " Mumbai"],
    "Kolkata": ["kolkata", "Calcutta", "KOLKATA "],
    "Bangalore": ["bangalore", "Bengaluru", " BANGALORE"],
    "Chennai": ["chennai", "Madras", "CHENNAI"],
    "Delhi": ["delhi", "New Delhi", " DELHI"],
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash on Delivery"]
PAYMENT_WEIGHTS = [0.22, 0.18, 0.38, 0.12, 0.10]

START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 12, 31)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def random_order_date():
    return START_DATE + timedelta(days=random.randint(0, DATE_RANGE_DAYS))


def build_base_rows(n):
    rows = []
    product_idx = np.random.randint(0, len(PRODUCTS), size=n)
    customer_idx = np.random.choice(N_CUSTOMERS, size=n, p=CUSTOMER_WEIGHTS)
    city_choices = np.random.choice(CITIES, size=n)
    quantities = np.random.randint(1, 11, size=n)
    discount_options = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30]
    discount_weights = [0.35, 0.20, 0.18, 0.12, 0.08, 0.04, 0.03]
    discounts = np.random.choice(discount_options, size=n, p=discount_weights)
    payments = np.random.choice(PAYMENT_METHODS, size=n, p=PAYMENT_WEIGHTS)
    delivery_days = np.random.randint(1, 11, size=n)

    for i in range(n):
        product_id, category, base_price = PRODUCTS[product_idx[i]]
        unit_price = round(base_price * np.random.uniform(0.9, 1.1), 2)
        quantity = int(quantities[i])
        discount = float(discounts[i])
        total_amount = round(quantity * unit_price * (1 - discount), 2)
        order_dt = random_order_date()
        delivery_dt = order_dt + timedelta(days=int(delivery_days[i]))
        city = city_choices[i]
        state = CITY_STATE[city]

        rows.append({
            "transaction_id": f"TXN{str(i + 1).zfill(7)}",
            "customer_id": CUSTOMER_IDS[customer_idx[i]],
            "product_id": product_id,
            "category": category,
            "city": city,
            "state": state,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "total_amount": total_amount,
            "payment_method": payments[i],
            "order_date": order_dt.isoformat(),
            "delivery_date": delivery_dt.isoformat(),
        })
    return rows


def inject_messiness(df):
    """Mutates a copy of df in place to introduce realistic data-quality issues."""
    df = df.copy()
    n = len(df)
    rng = np.random.default_rng(SEED)

    def sample_idx(frac):
        size = int(n * frac)
        return rng.choice(n, size=size, replace=False)

    # 1) Missing customer_id (~2%)
    df.loc[sample_idx(0.02), "customer_id"] = None

    # 2) Negative quantities (~1%) -- data entry / return errors
    neg_idx = sample_idx(0.01)
    df.loc[neg_idx, "quantity"] = -df.loc[neg_idx, "quantity"]

    # 3) Missing payment_method (~1.5%)
    df.loc[sample_idx(0.015), "payment_method"] = None

    # 4) Malformed / impossible order_date strings (~1%)
    bad_date_idx = sample_idx(0.01)
    bad_date_values = ["2024-13-45", "31-02-2024", "0000-00-00", "NaT", "2025/02/30", ""]
    df.loc[bad_date_idx, "order_date"] = [
        random.choice(bad_date_values) for _ in range(len(bad_date_idx))
    ]

    # 5) delivery_date earlier than order_date (~1%) -- date integrity issue
    bad_delivery_idx = sample_idx(0.01)
    valid_mask = ~df.index.isin(bad_date_idx)
    bad_delivery_idx = np.array([i for i in bad_delivery_idx if valid_mask[i]])
    if len(bad_delivery_idx) > 0:
        good_dates = pd.to_datetime(df.loc[bad_delivery_idx, "order_date"], errors="coerce")
        offsets = rng.integers(1, 6, size=len(bad_delivery_idx))
        df.loc[bad_delivery_idx, "delivery_date"] = [
            (d - timedelta(days=int(o))).date().isoformat() if pd.notna(d) else None
            for d, o in zip(good_dates, offsets)
        ]

    # 6) total_amount inconsistent with quantity * unit_price * (1 - discount) (~1%)
    inconsistent_idx = sample_idx(0.01)
    df.loc[inconsistent_idx, "total_amount"] = [
        round(v, 2) for v in rng.uniform(10, 500, size=len(inconsistent_idx))
    ]

    # 7) Missing category or city (~1.5% combined, randomly split)
    null_field_idx = sample_idx(0.015)
    for idx in null_field_idx:
        if rng.random() < 0.5:
            df.loc[idx, "category"] = None
        else:
            df.loc[idx, "city"] = None

    # 8) Inconsistent category casing/spacing (~3%)
    cat_dirty_idx = sample_idx(0.03)
    for idx in cat_dirty_idx:
        cat = df.loc[idx, "category"]
        if pd.isna(cat):
            continue
        variant = rng.choice(["upper", "lower", "leading_space", "trailing_space"])
        if variant == "upper":
            df.loc[idx, "category"] = cat.upper()
        elif variant == "lower":
            df.loc[idx, "category"] = cat.lower()
        elif variant == "leading_space":
            df.loc[idx, "category"] = f"  {cat}"
        else:
            df.loc[idx, "category"] = f"{cat}  "

    # 9) Legacy / inconsistent city aliases (~3%)
    city_dirty_idx = sample_idx(0.03)
    for idx in city_dirty_idx:
        city = df.loc[idx, "city"]
        if pd.isna(city) or city not in CITY_ALIASES:
            continue
        df.loc[idx, "city"] = random.choice(CITY_ALIASES[city])

    return df


def main():
    print(f"Generating {N_BASE_ROWS:,} base transactions...")
    base_rows = build_base_rows(N_BASE_ROWS)
    df = pd.DataFrame(base_rows)

    print("Injecting realistic data-quality issues...")
    df = inject_messiness(df)

    print(f"Appending {N_DUPLICATES:,} duplicate rows...")
    duplicate_rows = df.sample(n=N_DUPLICATES, random_state=SEED, replace=False)
    df = pd.concat([df, duplicate_rows], ignore_index=True)

    print("Shuffling final dataset...")
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    assert len(df) == TOTAL_ROWS, f"Expected {TOTAL_ROWS} rows, got {len(df)}"

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(df):,} rows to {OUTPUT_PATH}")

    corrupt_structurally_malformed_lines(OUTPUT_PATH, n_lines=50)


def corrupt_structurally_malformed_lines(path, n_lines=50):
    """
    Rewrites a handful of existing data lines so a typed numeric column
    (quantity / unit_price) holds a non-numeric token, simulating a corrupted
    upstream export (e.g. a leaked spreadsheet error code). Spark's CSV
    reader in PERMISSIVE mode treats a value that cannot be cast to the
    schema's declared type as a genuinely corrupted record -- unlike a
    differing token count, which Spark's CSV parser does NOT treat as
    malformed. This gives the ingestion stage real, schema-driven corrupt
    records to route into `_corrupt_record` instead of the pipeline body.
    """
    GARBAGE_TOKENS = ["#REF!", "ERROR", "N/A", "CORRUPTED", "#####"]

    with open(path, "r") as f:
        lines = f.readlines()

    header, data_lines = lines[0], lines[1:]
    n = len(data_lines)
    rng = np.random.default_rng(SEED + 1)
    target_idx = rng.choice(n, size=n_lines, replace=False)
    # quantity is column index 6, unit_price is column index 7
    target_cols = [6, 7]

    for i, idx in enumerate(target_idx):
        fields = data_lines[idx].rstrip("\n").split(",")
        col = target_cols[i % len(target_cols)]
        fields[col] = rng.choice(GARBAGE_TOKENS)
        data_lines[idx] = ",".join(fields) + "\n"

    with open(path, "w") as f:
        f.write(header)
        f.writelines(data_lines)

    print(f"Corrupted {n_lines} existing lines with non-numeric tokens in typed columns "
          f"to simulate malformed CSV records Spark will route via PERMISSIVE mode.")


if __name__ == "__main__":
    main()
