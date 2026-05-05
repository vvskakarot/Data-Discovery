"""
generate_data.py
----------------
Purpose: Generate realistic synthetic CSV data for the NL-to-SQL Insights Engine.
         Populates three Star Schema tables: users, events, subscriptions.

PM WHY — Faker over manual CSVs:
    Real-world data has patterns (churn spikes, seasonal signups, power users).
    Faker lets us simulate those patterns intentionally, making our AI queries
    more meaningful in demos. Static dummy data (user1, user2...) signals a
    junior prototype. Realistic data signals a product-minded engineer.

Run:
    python generate_data.py
Output:
    /data/users.csv
    /data/events.csv
    /data/subscriptions.csv
"""

import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

# ── Configuration ────────────────────────────────────────────────────────────

fake = Faker()
random.seed(42)
Faker.seed(42)

NUM_USERS         = 500
NUM_EVENTS        = 5_000   # ~10 events per user on average
OUTPUT_DIR        = "."

# Realistic product constants — these make AI-generated insights more interesting
SUBSCRIPTION_TIERS  = ["free", "pro", "enterprise"]
TIER_WEIGHTS        = [0.60, 0.30, 0.10]   # Freemium distribution
EVENT_TYPES         = [
    "page_view", "signup", "login", "feature_used",
    "report_generated", "query_run", "upgrade_clicked", "churned"
]
EVENT_WEIGHTS       = [0.35, 0.05, 0.20, 0.15, 0.10, 0.10, 0.03, 0.02]


# ── Helper Functions ──────────────────────────────────────────────────────────

def random_date(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


# ── Table Generators ──────────────────────────────────────────────────────────

def generate_users(n: int) -> pd.DataFrame:
    """
    Generate the DIMENSION table: users.

    Schema:
        user_id     (int)  — Primary Key
        name        (str)  — Full name
        email       (str)  — Unique email
        country     (str)  — Country of origin
        signup_date (date) — When user joined
        is_active   (bool) — Active in last 90 days
    """
    print(f"  Generating {n} users...")

    start_date = datetime(2022, 1, 1)
    end_date   = datetime(2024, 6, 30)

    records = []
    for user_id in range(1, n + 1):
        records.append({
            "user_id":     user_id,
            "name":        fake.name(),
            "email":       fake.unique.email(),
            "country":     fake.country(),
            "signup_date": random_date(start_date, end_date).strftime("%Y-%m-%d"),
            "is_active":   random.choices([True, False], weights=[0.72, 0.28])[0],
        })

    return pd.DataFrame(records)


def generate_subscriptions(users_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate the DIMENSION table: subscriptions.
    Each user has exactly one subscription record (1-to-1 with users).

    Schema:
        subscription_id (int)   — Primary Key
        user_id         (int)   — Foreign Key → users.user_id
        tier            (str)   — free | pro | enterprise
        start_date      (date)  — Subscription start
        end_date        (date)  — NULL if active (open-ended)
        mrr             (float) — Monthly Recurring Revenue in USD

    PM WHY — MRR column:
        MRR is the #1 SaaS metric. Including it lets us ask the AI
        "What is the total MRR by tier?" — a real business question
        that impresses interviewers.
    """
    print(f"  Generating {len(users_df)} subscription records...")

    # MRR mapping per tier
    mrr_map = {"free": 0.0, "pro": 29.99, "enterprise": 299.00}

    records = []
    for _, user in users_df.iterrows():
        tier       = random.choices(SUBSCRIPTION_TIERS, weights=TIER_WEIGHTS)[0]
        start      = datetime.strptime(user["signup_date"], "%Y-%m-%d")
        # ~20% of paid users have churned (end_date set); free users always "active"
        has_churned = (tier != "free") and (random.random() < 0.20)
        end_date   = random_date(start, datetime(2024, 12, 31)) if has_churned else None

        records.append({
            "subscription_id": user["user_id"],          # 1-to-1 simplification
            "user_id":         int(user["user_id"]),
            "tier":            tier,
            "start_date":      start.strftime("%Y-%m-%d"),
            "end_date":        end_date.strftime("%Y-%m-%d") if end_date else None,
            "mrr":             mrr_map[tier],
        })

    return pd.DataFrame(records)


def generate_events(users_df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Generate the FACT table: events.
    This is the largest table — every user action lands here.

    Schema:
        event_id    (int)  — Primary Key
        user_id     (int)  — Foreign Key → users.user_id
        event_type  (str)  — Type of action taken
        event_date  (date) — When it happened
        session_id  (str)  — Groups events within one session

    PM WHY — FACT table is separate:
        In a Star Schema, facts (things that HAPPEN) live apart from dimensions
        (things that EXIST). This lets analysts slice events by any user attribute
        without bloating the events table. It's the foundation of scalable
        analytics — what you'd pitch to a Data Engineer on a real team.
    """
    print(f"  Generating {n} events...")

    user_ids   = users_df["user_id"].tolist()
    start_date = datetime(2022, 1, 1)
    end_date   = datetime(2024, 12, 31)

    records = []
    for event_id in range(1, n + 1):
        records.append({
            "event_id":   event_id,
            "user_id":    random.choice(user_ids),
            "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0],
            "event_date": random_date(start_date, end_date).strftime("%Y-%m-%d"),
            "session_id": fake.uuid4(),   # UUID groups clicks in one session
        })

    return pd.DataFrame(records)


# ── Main Orchestration ────────────────────────────────────────────────────────

def main():
    print("\n🚀 NL-to-SQL Insights Engine — Data Generator")
    print("=" * 50)

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Generate tables in dependency order (users first — others reference it)
    users_df         = generate_users(NUM_USERS)
    subscriptions_df = generate_subscriptions(users_df)
    events_df        = generate_events(users_df, NUM_EVENTS)

    # 2. Save to CSV
    datasets = {
        "users":         users_df,
        "subscriptions": subscriptions_df,
        "events":        events_df,
    }

    print("\n  Saving CSVs...")
    for name, df in datasets.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  ✅ {path}  ({len(df):,} rows)")

    print("\n✨ Done! Your data is ready in /data\n")


if __name__ == "__main__":
    main()