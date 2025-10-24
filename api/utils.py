from datetime import datetime, timedelta
from functools import lru_cache
import os
import pandas as pd


def safe_qcut(series, n_bins=4, labels=None):
    unique_vals = series.nunique()
    if unique_vals < n_bins:
        n_bins = unique_vals
        if labels:
            labels = labels[-n_bins:]  # adjust labels if needed
    return pd.qcut(series, q=n_bins, labels=labels, duplicates="drop")


def compute_rfm(df, today=None):
    if today is None:
        today = pd.Timestamp(datetime.today().date())

        rfm = df.groupby("Sender Name").agg({
            "Created Date": lambda x: (today - x.max()).days,
            "Order Number": "nunique",
            "Net Extended Line Cost": "sum"
        }).reset_index()


    # Ensure date conversion
    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
    df.dropna(subset=["Created Date"], inplace=True)

    # Aggregate RFM metrics
    rfm = df.groupby("Sender Name").agg({
        "Created Date": lambda x: (today - x.max().date()).days,
        "Order Number": "nunique",
        "Net Extended Line Cost": "sum"
    }).reset_index()

    rfm.columns = ["Customer", "Recency", "Frequency", "Monetary"]

    # 🔧 FIX: use duplicates="drop" to avoid "Bin edges must be unique"
    rfm["R_Score"] = safe_qcut(rfm["Recency"], 4, labels=[4, 3, 2, 1])
    rfm["F_Score"] = safe_qcut(rfm["Frequency"], 4, labels=[1, 2, 3, 4])
    rfm["M_Score"] = safe_qcut(rfm["Monetary"], 4, labels=[1, 2, 3, 4])


    # Combine into RFM Score
    rfm["RFM_Score"] = (
        rfm["R_Score"].astype(int) +
        rfm["F_Score"].astype(int) +
        rfm["M_Score"].astype(int)
    )

    # Assign segment
    rfm["Segment"] = pd.cut(
        rfm["RFM_Score"],
        bins=[2, 5, 7, 9, 12],
        labels=["Low Value", "Mid Value", "High Value", "VIP"],
        include_lowest=True
    )

    return rfm

def compute_clv(df):
    clv = df.groupby("Sender Name").agg({
        "Order Number": "nunique",
        "Net Extended Line Cost": "sum"
    }).reset_index()

    clv["Avg Order Value"] = clv["Net Extended Line Cost"] / clv["Order Number"]
    clv["Purchase Frequency"] = clv["Order Number"] / len(df["Sender Name"].unique())
    clv["CLV"] = (clv["Avg Order Value"] * clv["Purchase Frequency"]) * 12  # e.g., 12 months projection

    return clv[["Sender Name", "CLV", "Avg Order Value", "Purchase Frequency"]]

def compute_churn(df, today=None, threshold_days=60):
    if today is None:
        today = datetime.today().date()

    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
    df.dropna(subset=["Created Date"], inplace=True)

    last_purchase = df.groupby("Sender Name")["Created Date"].max().reset_index()
    last_purchase["Days Since Last Purchase"] = (today - last_purchase["Created Date"].dt.date).dt.days
    last_purchase["Status"] = last_purchase["Days Since Last Purchase"].apply(
        lambda x: "Churned" if x > threshold_days else "Active"
    )

    churn_rate = round(
        (last_purchase["Status"].value_counts(normalize=True).get("Churned", 0) * 100), 2
    )
    return last_purchase, churn_rate

'''NEW HELPERS'''

# Default dataset path (can be overridden)
# DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "enriched_retail_dataset_500.csv")

# @lru_cache(maxsize=4)
# def load_dataset(file_path=DATA_FILE, date_column_candidates=None):

#     try:
#         # === Load entire dataset ===
#         df = pd.read_csv(file_path)

#         # === Clean column names ===
#         df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

#         # === Auto-detect or parse date columns ===
#         if date_column_candidates is None:
#             date_column_candidates = ["date", "order_date", "invoice_date", "delivery_date"]

#         for col in date_column_candidates:
#             if col in df.columns:
#                 try:
#                     df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
#                 except Exception:
#                     pass  # Skip if not convertible
#         # Pick main date column for analytics
#         date_cols = [c for c in date_column_candidates if c in df.columns]
#         if date_cols:
#             df["main_date"] = df[date_cols[0]]
#         else:
#             raise ValueError("No valid date column found in dataset.")

#         # === Drop empty or invalid date rows ===
#         df = df.dropna(subset=["main_date"])

#         # === Optional derived columns ===
#         df["month"] = df["main_date"].dt.to_period("M").astype(str)
#         df["week"] = df["main_date"].dt.to_period("W").astype(str)

#         print(f"✅ Loaded {len(df):,} rows from {os.path.basename(file_path)}")

#         return df

#     except Exception as e:
#         raise Exception(f"❌ Error loading dataset: {str(e)}")




DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "enriched_retail_dataset_500.csv")

@lru_cache(maxsize=4)
def load_dataset(file_path=DATA_FILE, date_column_candidates=None):
    try:
        # === Load entire dataset ===
        df = pd.read_csv(file_path)

        # === Normalize column names ===
        df.columns = (
            df.columns
            .str.strip()
            .str.replace(" ", "_")
            .str.replace(r"[^\w_]", "", regex=True)
            .str.lower()
            .str.replace(r"_+$", "", regex=True)
        )

        # === Detect date columns ===
        if date_column_candidates is None:
            date_column_candidates = [
                "date",
                "order_date",
                "invoice_date",
                "delivery_date",
                "transaction_date",
            ]

        # === Parse date columns ===
        for col in date_column_candidates:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                except Exception:
                    pass

        # === Determine the main date column ===
        valid_date_cols = [c for c in date_column_candidates if c in df.columns]
        if not valid_date_cols:
            raise ValueError(
                f"No valid date column found in dataset. Available columns: {df.columns.tolist()}"
            )

        df["main_date"] = df[valid_date_cols[0]]
        df = df.dropna(subset=["main_date"])

        # === Add derived helper columns ===
        df.loc[:, "month"] = df["main_date"].dt.to_period("M").astype(str)
        df.loc[:, "week"] = df["main_date"].dt.to_period("W").astype(str)

        print(f"✅ Loaded {len(df):,} rows from {os.path.basename(file_path)} using '{valid_date_cols[0]}' as main date.")

        return df

    except Exception as e:
        raise Exception(f"❌ Error loading dataset: {str(e)}")
