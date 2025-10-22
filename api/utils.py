from datetime import datetime, timedelta
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

