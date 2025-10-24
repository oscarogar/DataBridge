from datetime import datetime, timedelta
from functools import lru_cache
import os
import pandas as pd


# def safe_qcut(series, n_bins=4, labels=None):
#     unique_vals = series.nunique()
#     if unique_vals < n_bins:
#         n_bins = unique_vals
#         if labels:
#             labels = labels[-n_bins:]  # adjust labels if needed
#     return pd.qcut(series, q=n_bins, labels=labels, duplicates="drop")


# ===== SAFE QCUT UTILITY =====
def safe_qcut(series, q, labels):
    try:
        return pd.qcut(series, q, labels=labels, duplicates="drop")
    except ValueError:
        # fallback if not enough unique values
        return pd.Series([labels[0]] * len(series), index=series.index)

def compute_rfm(df, today=None, date_col="order_date"):
    if today is None:
        today = pd.Timestamp.today()  # <-- ensure Timestamp

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df.dropna(subset=[date_col], inplace=True)

    rfm = df.groupby("sender_name").agg({
        date_col: lambda x: (today - x.max()).days,  # x.max() is Timestamp, today is Timestamp
        "order_number": "nunique",
        "net_extended_line_cost": "sum"
    }).reset_index()

    rfm.columns = ["Customer", "Recency", "Frequency", "Monetary"]

    rfm["R_Score"] = safe_qcut(rfm["Recency"], 4, labels=[4,3,2,1])
    rfm["F_Score"] = safe_qcut(rfm["Frequency"], 4, labels=[1,2,3,4])
    rfm["M_Score"] = safe_qcut(rfm["Monetary"], 4, labels=[1,2,3,4])

    rfm["RFM_Score"] = rfm["R_Score"].astype(int) + rfm["F_Score"].astype(int) + rfm["M_Score"].astype(int)

    rfm["Segment"] = pd.cut(
        rfm["RFM_Score"],
        bins=[2,5,7,9,12],
        labels=["Low Value","Mid Value","High Value","VIP"],
        include_lowest=True
    )

    return rfm

def compute_clv(df):
    clv = df.groupby("sender_name").agg({
        "order_number":"nunique",
        "net_extended_line_cost":"sum"
    }).reset_index()

    clv["Avg Order Value"] = clv["net_extended_line_cost"] / clv["order_number"]
    clv["Purchase Frequency"] = clv["order_number"] / len(df["sender_name"].unique())
    clv["CLV"] = clv["Avg Order Value"] * clv["Purchase Frequency"] * 12

    return clv[["sender_name","CLV","Avg Order Value","Purchase Frequency"]]

def compute_churn(df, today=None, threshold_days=60, date_col="order_date"):
    if today is None:
        today = pd.Timestamp.today()  # <-- ensure Timestamp

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df.dropna(subset=[date_col], inplace=True)

    last_purchase = df.groupby("sender_name")[date_col].max().reset_index()
    last_purchase["Days Since Last Purchase"] = (today - last_purchase[date_col]).dt.days  # now safe
    last_purchase["Status"] = last_purchase["Days Since Last Purchase"].apply(
        lambda x: "Churned" if x > threshold_days else "Active"
    )

    churn_rate = round((last_purchase["Status"].value_counts(normalize=True).get("Churned",0)*100),2)
    return last_purchase, churn_rate

def compute_purchase_patterns(df, date_col="order_date"):

    # Ensure datetime
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df.dropna(subset=[date_col], inplace=True)

    patterns_list = []

    for sender, group in df.groupby("sender_name"):
        group = group.sort_values(by=date_col)

        # Compute average days between orders
        order_dates = group[date_col].drop_duplicates().sort_values()
        if len(order_dates) > 1:
            deltas = order_dates.diff().dt.days[1:]  # skip first NaT
            avg_days_between = round(deltas.mean(), 1)
        else:
            avg_days_between = 0.0

        # Compute average order value per sender
        avg_order_value = round(group["net_extended_line_cost"].sum() / group["order_number"].nunique(), 2)

        # Favorite product category (most frequent)
        if "product_category" in group.columns:
            favorite_category = group["product_category"].mode().iloc[0]
        else:
            favorite_category = None

        patterns_list.append({
            "sender_name": sender,
            "avg_days_between_orders": avg_days_between,
            "avg_order_value": avg_order_value,
            "favorite_category": favorite_category
        })

    # Convert to DataFrame for easier downstream processing
    patterns_df = pd.DataFrame(patterns_list)

    # ===== Optional: Orders by weekday & month =====
    orders_by_weekday = df.copy()
    orders_by_weekday["weekday"] = df[date_col].dt.day_name()
    orders_by_weekday = orders_by_weekday.groupby(["sender_name", "weekday"])["order_number"].nunique().reset_index(name="orders_count")

    orders_by_month = df.copy()
    orders_by_month["month"] = df[date_col].dt.to_period("M").astype(str)
    orders_by_month = orders_by_month.groupby(["sender_name", "month"])["order_number"].nunique().reset_index(name="orders_count")

    return patterns_df, orders_by_weekday, orders_by_month


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
