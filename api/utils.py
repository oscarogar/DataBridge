from datetime import datetime, timedelta
import pandas as pd
def compute_rfm(df, today=None):
    if today is None:
        today = datetime.today().date()

    rfm = df.groupby("Sender Name").agg({
        "Created Date": lambda x: (today - x.max().date()).days,  # Recency
        "Order Number": "nunique",                                # Frequency
        "Net Extended Line Cost": "sum"                           # Monetary
    }).reset_index()

    rfm.columns = ["Customer", "Recency", "Frequency", "Monetary"]

    # Optional: rank customers into quartiles for segmentation
    rfm["R_Score"] = pd.qcut(rfm["Recency"], 4, labels=[4,3,2,1])
    rfm["F_Score"] = pd.qcut(rfm["Frequency"].rank(method="first"), 4, labels=[1,2,3,4])
    rfm["M_Score"] = pd.qcut(rfm["Monetary"], 4, labels=[1,2,3,4])
    rfm["RFM_Score"] = rfm["R_Score"].astype(int) + rfm["F_Score"].astype(int) + rfm["M_Score"].astype(int)

    # Segment by value
    rfm["Segment"] = pd.cut(
        rfm["RFM_Score"],
        bins=[0,5,7,9,12],
        labels=["Low Value","Mid-Low","Mid-High","High Value"]
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

    last_purchase = df.groupby("Sender Name")["Created Date"].max().reset_index()
    last_purchase["Days Since Last Purchase"] = (today - last_purchase["Created Date"].dt.date).dt.days
    last_purchase["Status"] = last_purchase["Days Since Last Purchase"].apply(
        lambda x: "Churned" if x > threshold_days else "Active"
    )

    churn_rate = (last_purchase["Status"].value_counts(normalize=True).get("Churned", 0) * 100).round(2)

    return last_purchase, churn_rate

