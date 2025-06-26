from collections import Counter
from datetime import datetime, timedelta
from itertools import combinations
from django.shortcuts import render
import pandas as pd
from rest_framework.decorators import api_view
from rest_framework.response import Response
import openai
import os
from rest_framework import status
from dateutil.relativedelta import relativedelta
from django.utils.dateparse import parse_date
from django.http import JsonResponse
import sys

def python_version_view(request):
    return JsonResponse({"python_version": sys.version})

api_key = os.getenv("OPENAI_API_KEY")
EXCEL_PATH = os.path.join(os.path.dirname(__file__), 'data/data.xlsx')
SHEET_NAME = 'salesData'

client = openai.OpenAI(api_key=api_key) 

# Helper to load and parse data
def load_data():
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    df['Created Date'] = pd.to_datetime(df['Created Date'])
    return df

# Helper to generate OpenAI insights
def generate_insight(prompt_prefix, data_summary):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise Exception("OPENAI_API_KEY is not set in the environment.")

    client = openai.OpenAI(api_key=api_key)

    context = f"""You are a business analyst. Analyze the following data summary:
    {data_summary}
    {prompt_prefix}
    """

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert data analyst."},
            {"role": "user", "content": context}
        ]
    )
    return response.choices[0].message.content

# Helper to filter data by date

def filter_by_date(df, start_date, end_date):
    if start_date:
        start_date = pd.to_datetime(start_date)  # ensure datetime type
        df = df[df['Created Date'] >= start_date]
    if end_date:
        end_date = pd.to_datetime(end_date)
        df = df[df['Created Date'] <= end_date]
    return df

def safe_pct_change(row):
    current = row["current"]
    previous = row["previous"]
    if previous == 0:
        return "new" if current > 0 else 0
    return round(((current - previous) / previous) * 100, 2)


def parse_float(value):
    try:
        return float(str(value).replace(",", ""))
    except:
        return 0.0

@api_view(["GET"])
def sales_analytics(request):
    period = request.GET.get("period", "monthly")
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df_all = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Clean currency columns
    df_all["Net Extended Line Cost"] = df_all["Net Extended Line Cost"].apply(parse_float)
    df_all["Cost Price"] = df_all["Cost Price"].apply(parse_float)

    df_all["Created Date"] = pd.to_datetime(df_all["Created Date"])

    # Parse start and end dates
    try:
        start_date_dt = pd.to_datetime(start_date) if start_date else df_all["Created Date"].min()
        end_date_dt = pd.to_datetime(end_date) if end_date else df_all["Created Date"].max()
    except:
        return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)

    # Filter current period
    df_current = df_all[
        (df_all["Created Date"] >= start_date_dt) &
        (df_all["Created Date"] <= end_date_dt)
    ].copy()

    # Apply period granularity
    if period == "daily":
        df_current["Period"] = df_current["Created Date"].dt.date
        delta = timedelta(days=1)
    elif period == "weekly":
        df_current["Period"] = df_current["Created Date"].dt.to_period("W").apply(lambda r: r.start_time)
        delta = timedelta(weeks=1)
    elif period == "monthly":
        df_current["Period"] = df_current["Created Date"].dt.to_period("M").apply(lambda r: r.start_time)
        delta = relativedelta(months=1)
    elif period == "yearly":
        df_current["Period"] = df_current["Created Date"].dt.to_period("Y").apply(lambda r: r.start_time)
        delta = relativedelta(years=1)
    else:
        return Response({"error": "Invalid period. Use 'daily', 'weekly', 'monthly', or 'yearly'."}, status=status.HTTP_400_BAD_REQUEST)

    # ----------------------------
    # Current Period Metrics
    # ----------------------------
    total_sales_value = df_current["Net Extended Line Cost"].sum()
    total_orders = df_current["Order Number"].nunique()
    avg_order_value = total_sales_value / total_orders if total_orders else 0

    # ----------------------------
    # Compute Previous Period
    # ----------------------------
    range_length = end_date_dt - start_date_dt
    previous_start = start_date_dt - range_length
    previous_end = start_date_dt

    df_previous = df_all[
        (df_all["Created Date"] >= previous_start) &
        (df_all["Created Date"] < previous_end)
    ]

    sales_previous = df_previous["Net Extended Line Cost"].sum()
    sales_current = total_sales_value

    if sales_previous == 0:
        sales_growth = 100.0 if sales_current > 0 else 0
    else:
        sales_growth = ((sales_current - sales_previous) / abs(sales_previous)) * 100

    # ----------------------------
    # Performance Breakdown
    # ----------------------------
    performance_breakdown = (
        df_current.groupby("Period")["Net Extended Line Cost"]
        .sum()
        .sort_index()
        .reset_index()
        .rename(columns={"Net Extended Line Cost": "sales"})
    )
    performance_breakdown["sales"] = performance_breakdown["sales"].round(2)

    # ----------------------------
    # Top Products
    # ----------------------------
    top_products = (
        df_current.groupby("Product Description")["Net Extended Line Cost"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
        .rename(columns={"Net Extended Line Cost": "sales"})
    )
    top_products["sales"] = top_products["sales"].round(2)

    # ----------------------------
    # Customer Value
    # ----------------------------
    customer_value = (
        df_current.groupby("Sender Name")["Net Extended Line Cost"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"Net Extended Line Cost": "value"})
    )
    customer_value["value"] = customer_value["value"].round(2)

    return Response({
        "total_sales_value": round(total_sales_value, 2),
        "total_orders": total_orders,
        "avg_order_value": round(avg_order_value, 2),
        "sales_growth_percent": round(sales_growth, 2),
        "sales_performance_breakdown": performance_breakdown.to_dict(orient="records"),
        "top_products": top_products.to_dict(orient="records"),
        "customer_value": customer_value.to_dict(orient="records"),
    })

def get_sub_frequency(main_freq):
    if main_freq == "Q":
        return "M", "%B"
    elif main_freq == "M":
        return "W-MON", "Week %W"
    elif main_freq == "W-MON" or main_freq == "W":
        return "D", "%Y-%m-%d"
    elif main_freq == "D":
        return "H", "%H:%M"
    else:
        return "D", "%Y-%m-%d"  # default fallback

@api_view(["GET"])
def sales_trend_analytics(request):
    period = request.GET.get("period", "monthly")  # Optional: 'weekly', 'monthly', 'yearly'
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"])
    df["Net Extended Line Cost"] = df["Net Extended Line Cost"].apply(parse_float)

    today = df["Created Date"].max().normalize()

    # Determine custom or default period
    if start_date and end_date:
        try:
            start_current = pd.to_datetime(start_date)
            end_current = pd.to_datetime(end_date)
        except Exception:
            return Response({"error": "Invalid start_date or end_date format. Use YYYY-MM-DD."}, status=400)

        duration = end_current - start_current
        start_previous = start_current - duration - timedelta(days=1)
        end_previous = start_current - timedelta(days=1)

        # Dynamic frequency
        days = duration.days
        if days <= 7:
            freq = "D"
            label_format = "%Y-%m-%d"
        elif days <= 31:
            freq = "W-MON"
            label_format = "Week %W"
        elif days <= 365:
            freq = "M"
            label_format = "%B"
        else:
            freq = "Q"
            label_format = "Q%q %Y"
    else:
        # fallback to predefined period
        if period == "weekly":
            start_current = today - timedelta(days=today.weekday())
            end_current = start_current + timedelta(days=6)
            start_previous = start_current - timedelta(weeks=1)
            end_previous = start_current - timedelta(days=1)
            freq = "D"
            label_format = "%Y-%m-%d"
        elif period == "monthly":
            start_current = today.replace(day=1)
            end_current = (start_current + relativedelta(months=1)) - timedelta(days=1)
            start_previous = start_current - relativedelta(months=1)
            end_previous = start_current - timedelta(days=1)
            freq = "W-MON"
            label_format = "Week %W"
        elif period == "yearly":
            start_current = today.replace(month=1, day=1)
            end_current = today.replace(month=12, day=31)
            start_previous = start_current - relativedelta(years=1)
            end_previous = start_current - timedelta(days=1)
            freq = "M"
            label_format = "%B"
        else:
            return Response({"error": "Missing or invalid period. Provide either a period or start_date and end_date."}, status=400)

    # Filter periods
    df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)]
    df_previous = df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)]

    # Handle missing data
    if df_current.empty:
        return Response({"error": "No sales records found for the current period."}, status=404)

    if df_previous.empty:
        return Response({"error": "No sales records found for the previous period."}, status=404)

    # Totals
    total_sales_current = df_current["Net Extended Line Cost"].sum()
    total_sales_previous = df_previous["Net Extended Line Cost"].sum()

    growth_percent = (
        ((total_sales_current - total_sales_previous) / total_sales_previous) * 100
        if total_sales_previous != 0 else (100.0 if total_sales_current else 0.0)
    )

    def breakdown(df_slice, freq, label_format):
        df_slice = df_slice.copy()
        df_slice["Group"] = df_slice["Created Date"].dt.to_period(freq).dt.start_time
        summary = df_slice.groupby("Group")["Net Extended Line Cost"].sum().reset_index()
        summary.columns = ["period", "sales"]
        summary["label"] = summary["period"].dt.strftime(label_format)
        return summary.sort_values("period")

    current_breakdown = breakdown(df_current, freq, label_format)
    previous_breakdown = breakdown(df_previous, freq, label_format)

    # Compute percentage growth trend
    growth_trend_df = current_breakdown.copy()
    growth_trend_df["growth_percent"] = growth_trend_df["sales"].pct_change() * 100
    growth_trend_df["growth_percent"] = growth_trend_df["growth_percent"].round(2)
    growth_trend_df["growth_percent"] = growth_trend_df["growth_percent"].fillna(0.0)

    # Sub-period breakdowns
    sub_freq, sub_label_format = get_sub_frequency(freq)
    detailed_current_breakdown = breakdown(df_current, sub_freq, sub_label_format)
    detailed_previous_breakdown = breakdown(df_previous, sub_freq, sub_label_format)

    # Best-performing time
    best_time = current_breakdown.sort_values("sales", ascending=False).iloc[0].to_dict() if not current_breakdown.empty else {}

    # Product-level breakdown
    product_sales = (
        df_current.groupby("Product Description")["Net Extended Line Cost"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"Net Extended Line Cost": "sales"})
    )

    # Quarterly breakdown for long periods
    quarterly_breakdown = []
    if (period == "yearly") or (start_date and end_date and duration.days > 180):
        df_current["Quarter"] = df_current["Created Date"].dt.to_period("Q").dt.start_time
        quarterly_breakdown = (
            df_current.groupby("Quarter")["Net Extended Line Cost"]
            .sum()
            .reset_index()
            .rename(columns={"Net Extended Line Cost": "sales", "Quarter": "quarter"})
        )
        quarterly_breakdown["quarter"] = quarterly_breakdown["quarter"].dt.strftime("Q%q %Y")

    return Response({
    "period": period or "custom",
    "start_current": start_current.date(),
    "end_current": end_current.date(),
    "start_previous": start_previous.date(),
    "end_previous": end_previous.date(),
    "total_sales_current": round(total_sales_current, 2),
    "total_sales_previous": round(total_sales_previous, 2),
    "period_growth_percent": round(growth_percent, 2),
    "best_time_period": {
        "period": best_time.get("period"),
        "sales": round(best_time.get("sales", 0), 2),
        "label": best_time.get("label")
    } if best_time else {},
    "current_period_breakdown": current_breakdown.round(2).to_dict(orient="records"),
    "previous_period_breakdown": previous_breakdown.round(2).to_dict(orient="records"),
    "detailed_current_period_breakdown": detailed_current_breakdown.round(2).to_dict(orient="records"),
    "detailed_previous_period_breakdown": detailed_previous_breakdown.round(2).to_dict(orient="records"),
    "quarterly_breakdown": quarterly_breakdown.round(2).to_dict(orient="records") if not isinstance(quarterly_breakdown, list) else quarterly_breakdown,
    "product_sales_breakdown": product_sales.round(2).to_dict(orient="records"),
    "growth_trend": growth_trend_df[["label", "sales", "growth_percent"]].round(2).to_dict(orient="records"),
    })

@api_view(["GET"])
def profit_margin_analytics(request):
    period = request.GET.get("period", "monthly")
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"])
    df["Cost Price"] = df["Cost Price"].replace(",", "", regex=True).astype(float)
    df["Net Extended Line Cost"] = df["Net Extended Line Cost"].replace(",", "", regex=True).astype(float)
    df["Requested Qty"] = df["Requested Qty"].astype(float)

    # Calculate Profit & Margin
    df["Profit"] = df["Net Extended Line Cost"] - (df["Cost Price"] * df["Requested Qty"])
    df["Profit Margin"] = df["Profit"] / df["Net Extended Line Cost"].replace(0, pd.NA) * 100

    today = df["Created Date"].max().normalize()

    # Determine current and previous periods
    if start_date and end_date:
        start_current = pd.to_datetime(start_date)
        end_current = pd.to_datetime(end_date)
        duration = end_current - start_current
        start_previous = start_current - duration - timedelta(days=1)
        end_previous = start_current - timedelta(days=1)

        days = duration.days
        if days <= 7:
            freq = "D"
            label_format = "%Y-%m-%d"
        elif days <= 31:
            freq = "W-MON"
            label_format = "Week %W"
        elif days <= 365:
            freq = "M"
            label_format = "%B"
        else:
            freq = "Q"
            label_format = "Q%q %Y"
    else:
        if period == "weekly":
            start_current = today - timedelta(days=today.weekday())
            end_current = start_current + timedelta(days=6)
            start_previous = start_current - timedelta(weeks=1)
            end_previous = start_current - timedelta(days=1)
            freq = "D"
            label_format = "%Y-%m-%d"
        elif period == "monthly":
            start_current = today.replace(day=1)
            end_current = (start_current + relativedelta(months=1)) - timedelta(days=1)
            start_previous = start_current - relativedelta(months=1)
            end_previous = start_current - timedelta(days=1)
            freq = "W-MON"
            label_format = "Week %W"
        elif period == "yearly":
            start_current = today.replace(month=1, day=1)
            end_current = today.replace(month=12, day=31)
            start_previous = start_current - relativedelta(years=1)
            end_previous = start_current - timedelta(days=1)
            freq = "M"
            label_format = "%B"
        else:
            return Response({"error": "Missing or invalid period. Provide either a valid 'period' or both 'start_date' and 'end_date'."}, status=400)

    # Slice data
    df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)]
    df_previous = df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)]

    if df_current.empty:
        return Response({"error": "No profit data found for the current period."}, status=404)
    if df_previous.empty:
        return Response({"error": "No profit data found for the previous period."}, status=404)

    # Aggregates
    profit_current = df_current["Profit"].sum()
    revenue_current = df_current["Net Extended Line Cost"].sum()
    profit_margin_current = (profit_current / revenue_current * 100) if revenue_current else 0

    profit_previous = df_previous["Profit"].sum()
    revenue_previous = df_previous["Net Extended Line Cost"].sum()
    profit_margin_previous = (profit_previous / revenue_previous * 100) if revenue_previous else 0

    profit_growth = ((profit_current - profit_previous) / profit_previous * 100) if profit_previous else (100 if profit_current else 0)

    def breakdown(df_slice):
        df_slice = df_slice.copy()
        df_slice["Period"] = df_slice["Created Date"].dt.to_period(freq).dt.start_time
        summary = df_slice.groupby("Period").agg(
            revenue=("Net Extended Line Cost", "sum"),
            cost=("Cost Price", lambda x: (x * df_slice.loc[x.index, "Requested Qty"]).sum()),
            profit=("Profit", "sum"),
        ).reset_index()
        summary["label"] = summary["Period"].dt.strftime(label_format)
        summary["profit_margin"] = summary.apply(
            lambda row: (row["profit"] / row["revenue"] * 100) if row["revenue"] else 0, axis=1
        )
        return summary.round(2).sort_values("Period")  # <-- round all float columns to 2dp

    current_breakdown = breakdown(df_current)
    previous_breakdown = breakdown(df_previous)

    return Response({
        "period": period or "custom",
        "start_current": start_current.date(),
        "end_current": end_current.date(),
        "start_previous": start_previous.date(),
        "end_previous": end_previous.date(),
        "total_profit_current": round(profit_current, 2),
        "total_profit_previous": round(profit_previous, 2),
        "profit_growth_percent": round(profit_growth, 2),
        "profit_margin_current": round(profit_margin_current, 2),
        "profit_margin_previous": round(profit_margin_previous, 2),
        "current_period_breakdown": current_breakdown.to_dict(orient="records"),
        "previous_period_breakdown": previous_breakdown.to_dict(orient="records"),
    })

@api_view(['GET'])
def cost_analysis(request):
    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    # Clean columns
    df["Net Extended Line Cost"] = df["Net Extended Line Cost"].apply(parse_float)
    df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')

    # Query params
    period = request.query_params.get("period")
    store_filter = request.query_params.get("store")
    product_filter = request.query_params.get("product")
    start_date_param = request.query_params.get("start_date")
    end_date_param = request.query_params.get("end_date")

    today = pd.Timestamp.today().normalize()
    trend_freq = "W"

    try:
        if start_date_param and end_date_param:
            start_current = pd.to_datetime(start_date_param)
            end_current = pd.to_datetime(end_date_param)

            if start_current > end_current:
                return Response({"error": "start_date cannot be after end_date."}, status=400)

            delta_days = (end_current - start_current).days
            if delta_days <= 14:
                trend_freq = "D"
            elif delta_days <= 60:
                trend_freq = "W"
            else:
                trend_freq = "M"

            start_previous = end_previous = None

        elif period == "week":
            start_current = today - pd.to_timedelta(today.weekday(), unit='d')
            end_current = start_current + pd.Timedelta(days=6)
            start_previous = start_current - pd.Timedelta(days=7)
            end_previous = start_previous + pd.Timedelta(days=6)
            trend_freq = "D"

        elif period == "month":
            start_current = today.replace(day=1)
            end_current = start_current + pd.offsets.MonthEnd(1)
            start_previous = (start_current - pd.offsets.MonthBegin(1)).replace(day=1)
            end_previous = start_previous + pd.offsets.MonthEnd(1)
            trend_freq = "W"

        elif period == "year":
            start_current = today.replace(month=1, day=1)
            end_current = start_current + pd.offsets.YearEnd(1)
            start_previous = (start_current - pd.offsets.YearBegin(1)).replace(month=1, day=1)
            end_previous = start_previous + pd.offsets.YearEnd(1)
            trend_freq = "M"

        else:
            return Response({"error": "Provide valid 'period' or 'start_date' and 'end_date'."}, status=400)

    except Exception as e:
        return Response({"error": f"Invalid date input: {str(e)}"}, status=400)

    # Date bounds check
    min_date = df["Created Date"].min()
    max_date = df["Created Date"].max()
    if start_current > max_date or end_current < min_date:
        return Response({
            "error": "Provided date range is outside the available data range.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    # Filter current and previous periods
    df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)].copy()
    df_previous = (
        df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)].copy()
        if start_previous and end_previous else pd.DataFrame(columns=df.columns)
    )

    if df_current.empty:
        return Response({"error": "No data available for the current period."}, status=404)

    # Apply filters
    if store_filter:
        df_current = df_current[df_current["Store Name"].str.lower() == store_filter.lower()]
    if product_filter:
        df_current = df_current[df_current["Product Description"].str.lower() == product_filter.lower()]

    # Proceed with analysis
    total_cost_current = df_current["Net Extended Line Cost"].sum()
    total_cost_previous = df_previous["Net Extended Line Cost"].sum()
    growth_percent = (
        ((total_cost_current - total_cost_previous) / total_cost_previous) * 100
        if total_cost_previous else 0
    )

    trend_current = df_current.set_index("Created Date")["Net Extended Line Cost"].resample(trend_freq).sum().reset_index()
    trend_current["Net Extended Line Cost"] = trend_current["Net Extended Line Cost"].round(2)

    trend_previous = (
        df_previous.set_index("Created Date")["Net Extended Line Cost"].resample(trend_freq).sum().reset_index()
        if not df_previous.empty else []
    )
    if not isinstance(trend_previous, list):
        trend_previous["Net Extended Line Cost"] = trend_previous["Net Extended Line Cost"].round(2)

    # Breakdown & summaries
    product_costs = (
        df_current.groupby("Product Description")["Net Extended Line Cost"]
        .sum().reset_index().rename(columns={"Net Extended Line Cost": "Total Cost"})
        .sort_values("Total Cost", ascending=False)
    )
    product_costs["Total Cost"] = product_costs["Total Cost"].round(2)

    store_costs = (
        df_current.groupby("Store Name")["Net Extended Line Cost"]
        .sum().reset_index().rename(columns={"Net Extended Line Cost": "Total Cost"})
        .sort_values("Total Cost", ascending=False)
    )
    store_costs["Total Cost"] = store_costs["Total Cost"].round(2)

    product_count_per_store = (
        df_current.groupby("Store Name")["Product Description"]
        .nunique().reset_index().rename(columns={"Product Description": "Unique Product Count"})
    )

    # Safely round only numeric fields
    if not product_costs.empty:
        row = product_costs.iloc[0]
        most_expensive_product = {
            "Product Description": row["Product Description"],
            "Total Cost": round(row["Total Cost"], 2)
        }
    else:
        most_expensive_product = {}

    if not store_costs.empty:
        row = store_costs.iloc[0]
        most_expensive_store = {
            "Store Name": row["Store Name"],
            "Total Cost": round(row["Total Cost"], 2)
        }
    else:
        most_expensive_store = {}

    return Response({
        "mode": "custom" if start_date_param else period,
        "start_current": start_current.date(),
        "end_current": end_current.date(),
        "start_previous": start_previous.date() if start_previous else None,
        "end_previous": end_previous.date() if end_previous else None,
        "total_cost_current": round(total_cost_current, 2),
        "total_cost_previous": round(total_cost_previous, 2) if not df_previous.empty else None,
        "cost_growth_percent": round(growth_percent, 2) if not df_previous.empty else None,
        "current_period_cost_trend": trend_current.to_dict(orient="records"),
        "previous_period_cost_trend": trend_previous if isinstance(trend_previous, list) else trend_previous.to_dict(orient="records"),
        "product_cost_breakdown": product_costs.to_dict(orient="records"),
        "store_cost_breakdown": store_costs.to_dict(orient="records"),
        "products_involved": sorted(df_current["Product Description"].dropna().unique().tolist()),
        "stores_involved": sorted(df_current["Store Name"].dropna().unique().tolist()),
        "unique_products_per_store": product_count_per_store.to_dict(orient="records"),
        "most_expensive_product": most_expensive_product,
        "most_expensive_store": most_expensive_store,
        "filters_applied": {
            "store": store_filter,
            "product": product_filter,
            "start_date": start_date_param,
            "end_date": end_date_param
        }
    })

@api_view(['GET'])
def sales_summary(request):
    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    # Clean and prepare DataFrame
    df['Created Date'] = pd.to_datetime(df['Created Date'], errors='coerce')
    df['Net Extended Line Cost'] = pd.to_numeric(df['Net Extended Line Cost'].astype(str).str.replace(',', ''), errors='coerce')
    df['Requested Qty'] = pd.to_numeric(df['Requested Qty'], errors='coerce')

    # Parse query parameters
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')

    try:
        start_date = pd.to_datetime(start_date_str) if start_date_str else None
        end_date = pd.to_datetime(end_date_str) if end_date_str else None
        if start_date and end_date and start_date > end_date:
            return Response({"error": "start_date cannot be after end_date."}, status=400)
    except Exception as e:
        return Response({"error": f"Invalid date format. Use YYYY-MM-DD. Details: {str(e)}"}, status=400)

    # Validate that requested dates fall within dataset range
    min_date = df['Created Date'].min()
    max_date = df['Created Date'].max()
    if (start_date and start_date > max_date) or (end_date and end_date < min_date):
        return Response({
            "error": "Provided date range is outside the available data range.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    # Filter by date
    if start_date:
        df = df[df['Created Date'] >= start_date]
    if end_date:
        df = df[df['Created Date'] <= end_date]

    if df.empty:
        return Response({"message": "No data available for the specified period."}, status=404)

    # Summary calculations
    total_orders = df['Order Number'].nunique()
    new_orders = df[['Order Number', 'Created Date']].drop_duplicates().shape[0]
    total_revenue = df['Net Extended Line Cost'].sum()
    avg_sales = df['Net Extended Line Cost'].mean()

    # Top products
    top_products = (
        df.groupby('Product Description')
        .agg(
            total_sales=('Net Extended Line Cost', 'sum'),
            quantity_sold=('Requested Qty', 'sum')
        )
        .sort_values(by='total_sales', ascending=False)
        .reset_index()
        .head(5)
    )

    # Round float values in top products
    top_products['total_sales'] = top_products['total_sales'].round(2)
    top_products['quantity_sold'] = top_products['quantity_sold'].round(2)

    return Response({
        "summary": {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "total_orders": total_orders,
            "new_orders": new_orders,
            "total_revenue": round(total_revenue, 2),
            "average_sales": round(avg_sales, 2)
        },
        "top_products": top_products.to_dict(orient='records')
    })

@api_view(["GET"])
def transaction_summary(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    if not start_date or not end_date:
        return Response({"error": "start_date and end_date are required"}, status=400)

    try:
        start_date_parsed = pd.to_datetime(start_date)
        end_date_parsed = pd.to_datetime(end_date)
        if start_date_parsed > end_date_parsed:
            return Response({"error": "start_date cannot be after end_date."}, status=400)
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
    df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"].astype(str).str.replace(",", ""), errors="coerce")
    df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce")

    # Check available range
    min_date = df["Created Date"].min()
    max_date = df["Created Date"].max()
    if (start_date_parsed > max_date) or (end_date_parsed < min_date):
        return Response({
            "error": "Provided date range is outside the available dataset.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    # Current period
    current_df = df[(df["Created Date"] >= start_date_parsed) & (df["Created Date"] <= end_date_parsed)]
    if current_df.empty:
        return Response({"message": "No transactions found in the current period."}, status=404)

    current_total_value = current_df['Net Extended Line Cost'].sum()
    current_total_quantity = current_df['Requested Qty'].sum()
    current_avg_order_value = current_df.groupby('Order Number')['Net Extended Line Cost'].sum().mean()

    # Previous period
    duration = end_date_parsed - start_date_parsed
    prev_start = start_date_parsed - duration - timedelta(days=1)
    prev_end = start_date_parsed - timedelta(days=1)

    previous_df = df[(df["Created Date"] >= prev_start) & (df["Created Date"] <= prev_end)]
    previous_total_value = previous_df['Net Extended Line Cost'].sum()
    previous_total_quantity = previous_df['Requested Qty'].sum()
    previous_avg_order_value = previous_df.groupby('Order Number')['Net Extended Line Cost'].sum().mean()

    # Store-level summary (Top 20)
    def store_summary(subset_df):
        summary = subset_df.groupby('Store Name').agg({
            'Requested Qty': 'sum',
            'Net Extended Line Cost': 'sum'
        }).round(2).reset_index()
        summary['Requested Qty'] = summary['Requested Qty'].round(2)
        summary['Net Extended Line Cost'] = summary['Net Extended Line Cost'].round(2)
        return summary.sort_values('Net Extended Line Cost', ascending=False).head(20).to_dict(orient='records')

    # Product-level summary (Top 20)
    def product_summary(subset_df):
        summary = subset_df.groupby('Product Description').agg({
            'Requested Qty': 'sum',
            'Net Extended Line Cost': 'sum'
        }).round(2).reset_index()
        summary['Requested Qty'] = summary['Requested Qty'].round(2)
        summary['Net Extended Line Cost'] = summary['Net Extended Line Cost'].round(2)
        return summary.sort_values('Net Extended Line Cost', ascending=False).head(20).to_dict(orient='records')

    # Trend chart
    def trend_chart(subset_df):
        trend_df = subset_df.groupby(subset_df['Created Date'].dt.date)['Net Extended Line Cost'].sum().reset_index()
        trend_df['Net Extended Line Cost'] = trend_df['Net Extended Line Cost'].round(2)
        return trend_df.rename(columns={'Created Date': 'date', 'Net Extended Line Cost': 'revenue'}).to_dict(orient='records')

    # Percentage change helper
    def percentage_change(current, previous):
        if previous == 0:
            return None
        return round(((current - previous) / previous) * 100, 2)

    return Response({
        "start_date": str(start_date_parsed.date()),
        "end_date": str(end_date_parsed.date()),
        "current_period": {
            "total_transaction_value": round(current_total_value, 2),
            "total_quantity": round(current_total_quantity, 2),
            "average_order_value": round(current_avg_order_value or 0, 2),
            "store_summary": store_summary(current_df),
            "product_summary": product_summary(current_df),
            "trend_chart": trend_chart(current_df),
        },
        "previous_period": {
            "total_transaction_value": round(previous_total_value, 2),
            "total_quantity": round(previous_total_quantity, 2),
            "average_order_value": round(previous_avg_order_value or 0, 2),
            "store_summary": store_summary(previous_df),
            "product_summary": product_summary(previous_df),
            "trend_chart": trend_chart(previous_df),
        },
        "percentage_changes": {
            "transaction_value_change": percentage_change(current_total_value, previous_total_value),
            "quantity_change": percentage_change(current_total_quantity, previous_total_quantity),
            "average_order_value_change": percentage_change(current_avg_order_value, previous_avg_order_value),
        }
    })


@api_view(["GET"])
def transaction_entities_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    store_filter = request.GET.get("store")
    sender_filter = request.GET.get("sender")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
    df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"].astype(str).str.replace(",", ""), errors="coerce")
    df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce")
    df = df.dropna(subset=["Created Date"])

    try:
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df["Created Date"] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df["Created Date"] <= end]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    if store_filter:
        df = df[df["Store Name"] == store_filter]
    if sender_filter:
        df = df[df["Sender Name"] == sender_filter]

    if df.empty:
        return Response({"message": "No data found for selected filters"}, status=200)

    total_revenue = df["Net Extended Line Cost"].sum()

    try:
        store_group = df.groupby("Store Name").agg(
            revenue=("Net Extended Line Cost", "sum"),
            orders=("Order Number", "nunique"),
            quantity=("Requested Qty", "sum")
        )
        store_group["avg_order_value"] = store_group["revenue"] / store_group["orders"]
        store_group["revenue_pct"] = (store_group["revenue"] / total_revenue * 100).round(2)
        top_stores = store_group.sort_values("revenue", ascending=False).head(5).round(2).to_dict("index")

        customer_group = df.groupby("Sender Name").agg(
            revenue=("Net Extended Line Cost", "sum"),
            orders=("Order Number", "nunique"),
            quantity=("Requested Qty", "sum")
        )
        customer_group["avg_order_value"] = customer_group["revenue"] / customer_group["orders"]
        customer_group["revenue_pct"] = (customer_group["revenue"] / total_revenue * 100).round(2)
        top_customers_df = customer_group.sort_values("revenue", ascending=False).head(5).round(2)
        top_customers = top_customers_df.to_dict("index")

        df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
        top_customer_names = list(top_customers_df.index)
        customer_trend = (
            df[df["Sender Name"].isin(top_customer_names)]
            .groupby(["Sender Name", "Month"])["Net Extended Line Cost"]
            .sum().reset_index()
            .pivot(index="Month", columns="Sender Name", values="Net Extended Line Cost")
            .fillna(0).round(2)
        )
        customer_trend_data = customer_trend.reset_index().to_dict(orient="records")

        product_group = df.groupby("Product Description").agg(
            revenue=("Net Extended Line Cost", "sum"),
            quantity=("Requested Qty", "sum"),
            orders=("Order Number", "nunique")
        )
        product_group["revenue_pct"] = (product_group["revenue"] / total_revenue * 100).round(2)
        top_products = product_group.sort_values("revenue", ascending=False).head(5).round(2).to_dict("index")

    except Exception as e:
        return Response({"error": f"Failed during aggregation: {str(e)}"}, status=500)

    return Response({
        "filters_applied": {
            "start_date": start_date,
            "end_date": end_date,
            "store_filter": store_filter,
            "sender_filter": sender_filter
        },
        "top_stores": top_stores,
        "top_customers": top_customers,
        "top_products_by_revenue": top_products,
        "monthly_customer_trend": customer_trend_data
    })

@api_view(["GET"])
def transaction_timing_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
    df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors="coerce")
    df = df.dropna(subset=["Created Date"])

    try:
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df["Created Date"] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df["Created Date"] <= end]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No transactions found for the selected period"}, status=200)

    try:
        df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
        df["Week"] = df["Created Date"].dt.to_period("W").astype(str)
        df["Day"] = df["Created Date"].dt.date.astype(str)
        df["Weekday"] = df["Created Date"].dt.day_name()
        df["Hour"] = df["Created Date"].dt.hour

        freq_by_month = df.groupby("Month").size().to_dict()
        freq_by_week = df.groupby("Week").size().to_dict()
        freq_by_day = df.groupby("Day").size().to_dict()
        freq_by_weekday = df.groupby("Weekday").size().sort_values(ascending=False).to_dict()
        freq_by_hour = df.groupby("Hour").size().sort_index().to_dict()

        df = df.dropna(subset=["Delivery Date"])
        df["Fulfillment Days"] = (df["Delivery Date"] - df["Created Date"]).dt.days

        avg_fulfillment = df["Fulfillment Days"].mean()
        best_fulfillment = df["Fulfillment Days"].min()
        worst_fulfillment = df["Fulfillment Days"].max()

        df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
        fulfillment_trend = df.groupby("Month")["Fulfillment Days"].mean().round(2).to_dict()

    except Exception as e:
        return Response({"error": f"Error during aggregation: {str(e)}"}, status=500)

    return Response({
        "frequency": {
            "by_month": freq_by_month,
            "by_week": freq_by_week,
            "by_day": freq_by_day,
            "by_weekday": freq_by_weekday,
            "by_hour": freq_by_hour
        },
        "fulfillment": {
            "average_days": round(avg_fulfillment, 2) if not pd.isna(avg_fulfillment) else None,
            "fastest_days": int(best_fulfillment) if not pd.isna(best_fulfillment) else None,
            "slowest_days": int(worst_fulfillment) if not pd.isna(worst_fulfillment) else None,
            "monthly_trend": fulfillment_trend
        }
    })

@api_view(["GET"])
def product_demand_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
    df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce")
    df = df.dropna(subset=["Created Date", "Requested Qty"])

    try:
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df["Created Date"] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df["Created Date"] <= end]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No data found in the selected period."}, status=200)

    try:
        df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
        df["Weekday"] = df["Created Date"].dt.day_name()

        top_products = df.groupby("Product Description")["Requested Qty"].sum().sort_values(ascending=False).head(10)
        trend = df.groupby(df["Created Date"].dt.date)["Requested Qty"].sum().reset_index()
        trend.columns = ["date", "quantity"]

        store_demand = (
            df.groupby(["Store Name", "Product Description"])["Requested Qty"].sum()
            .reset_index().sort_values(by="Requested Qty", ascending=False)
        )

        product_order_qty = df.groupby(["Product Description", "Order Number"])["Requested Qty"].sum().reset_index()
        velocity = product_order_qty.groupby("Product Description")["Requested Qty"].mean().sort_values(ascending=False).head(10)

        by_month = df.groupby("Month")["Requested Qty"].sum().to_dict()
        by_weekday = df.groupby("Weekday")["Requested Qty"].sum().sort_values(ascending=False).to_dict()

        parsed_start = pd.to_datetime(parse_date(start_date)) if start_date else df["Created Date"].min()
        parsed_end = pd.to_datetime(parse_date(end_date)) if end_date else df["Created Date"].max()

        if not parsed_start or not parsed_end:
            raise ValueError("Invalid start or end date for comparison")
        period_length = parsed_end - parsed_start

        prev_start = parsed_start - period_length
        prev_end = parsed_start - timedelta(days=1)
        prev_df = df[(df["Created Date"] >= prev_start) & (df["Created Date"] <= prev_end)]

        recent_top = df.groupby("Product Description")["Requested Qty"].sum()
        prev_top = prev_df.groupby("Product Description")["Requested Qty"].sum()

        combined_demand = pd.concat([recent_top, prev_top], axis=1, keys=["current", "previous"]).fillna(0)
        combined_demand["pct_change"] = ((combined_demand["current"] - combined_demand["previous"]) /
                                          combined_demand["previous"].replace(0, 1)) * 100
        rising_demand = combined_demand.sort_values("pct_change", ascending=False).head(5).round(2).to_dict(orient="index")

        matrix = df.pivot_table(index="Store Name", columns="Product Description", values="Requested Qty", aggfunc="sum", fill_value=0)

    except Exception as e:
        return Response({"error": f"Failed to compute demand analysis: {str(e)}"}, status=500)

    return Response({
        "top_products_by_quantity": top_products.to_dict(),
        "demand_trend_over_time": trend.to_dict(orient="records"),
        "store_product_demand": store_demand.to_dict(orient="records"),
        "demand_velocity_per_product": velocity.round(2).to_dict(),
        "seasonality": {
            "monthly_demand": by_month,
            "weekday_demand": by_weekday
        },
        "rising_product_demand": rising_demand,
        "product_demand_matrix": matrix.astype(int).to_dict()
    })

@api_view(["GET"])
def product_revenue_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
    df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"].astype(str).str.replace(",", ""), errors='coerce')
    df = df.dropna(subset=["Created Date", "Net Extended Line Cost"])

    try:
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df["Created Date"] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df["Created Date"] <= end]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No data found in the selected period."}, status=200)

    try:
        df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
        df["Weekday"] = df["Created Date"].dt.day_name()

        top_products = df.groupby("Product Description")["Net Extended Line Cost"].sum().sort_values(ascending=False).head(10)
        trend = df.groupby(df["Created Date"].dt.date)["Net Extended Line Cost"].sum().reset_index()
        trend.columns = ["date", "revenue"]

        store_revenue = (
            df.groupby(["Store Name", "Product Description"])["Net Extended Line Cost"]
            .sum().reset_index().sort_values(by="Net Extended Line Cost", ascending=False)
        )

        product_order_revenue = df.groupby(["Product Description", "Order Number"])["Net Extended Line Cost"].sum().reset_index()
        revenue_yield = product_order_revenue.groupby("Product Description")["Net Extended Line Cost"].mean().sort_values(ascending=False).head(10)

        by_month = df.groupby("Month")["Net Extended Line Cost"].sum().to_dict()
        by_weekday = df.groupby("Weekday")["Net Extended Line Cost"].sum().sort_values(ascending=False).to_dict()

        parsed_start = pd.to_datetime(parse_date(start_date)) if start_date else df["Created Date"].min()
        parsed_end = pd.to_datetime(parse_date(end_date)) if end_date else df["Created Date"].max()
        if not parsed_start or not parsed_end:
            raise ValueError("Invalid start or end date for comparison")
        period_length = parsed_end - parsed_start

        prev_start = parsed_start - period_length
        prev_end = parsed_start - timedelta(days=1)
        prev_df = df[(df["Created Date"] >= prev_start) & (df["Created Date"] <= prev_end)]

        recent_revenue = df.groupby("Product Description")["Net Extended Line Cost"].sum()
        prev_revenue = prev_df.groupby("Product Description")["Net Extended Line Cost"].sum()

        combined_revenue = pd.concat([recent_revenue, prev_revenue], axis=1, keys=["current", "previous"]).fillna(0)

        def safe_pct_change(row):
            current = row["current"]
            previous = row["previous"]
            if previous == 0:
                return "new" if current > 0 else 0
            return round(((current - previous) / previous) * 100, 2)

        combined_revenue["pct_change"] = combined_revenue.apply(safe_pct_change, axis=1)

        rising_revenue = (
            combined_revenue.sort_values(
                by="pct_change", ascending=False, key=lambda x: x.map(lambda v: float('-inf') if v == 0 else (float('inf') if v == "new" else v))
            )
            .head(5)
            .round(2)
            .to_dict(orient="index")
        )

        matrix = df.pivot_table(index="Store Name", columns="Product Description", values="Net Extended Line Cost", aggfunc="sum", fill_value=0)

    except Exception as e:
        return Response({"error": f"Failed to compute revenue analysis: {str(e)}"}, status=500)

    return Response({
        "top_products_by_revenue": top_products.round(2).to_dict(),
        "revenue_trend_over_time": trend.round(2).to_dict(orient="records"),
        "store_product_revenue": store_revenue.round(2).to_dict(orient="records"),
        "revenue_yield_per_product": revenue_yield.round(2).to_dict(),
        "seasonality": {
            "monthly_revenue": {k: round(v, 2) for k, v in by_month.items()},
            "weekday_revenue": {k: round(v, 2) for k, v in by_weekday.items()}
        },
        "rising_product_revenue": rising_revenue,
        "product_revenue_matrix": matrix.round(2).to_dict()
    })


@api_view(["GET"])
def product_correlation_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    store_filter = request.GET.get("store")
    sender_filter = request.GET.get("sender")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    try:
        df = filter_by_date(df, start_date, end_date)
        if store_filter:
            df = df[df["Store Name"].str.lower() == store_filter.lower()]
        if sender_filter:
            df = df[df["Sender Name"].str.lower() == sender_filter.lower()]
    except Exception as e:
        return Response({"error": f"Error during filtering: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No data available for the selected filters."}, status=200)

    try:
        basket = df.groupby(["Order Number", "Product Description"])["Requested Qty"].sum().unstack(fill_value=0)
        if basket.empty or basket.shape[1] < 2:
            return Response({"message": "Not enough overlapping product orders to compute correlation."}, status=200)

        binary_basket = (basket > 0).astype(int)
        correlation_matrix = binary_basket.corr().fillna(0).round(3)

        # Pairs
        order_groups = df.groupby("Order Number")["Product Description"].apply(set)
        pairs = []
        for products in order_groups:
            if len(products) > 1:
                pairs.extend(combinations(products, 2))

        if not pairs:
            return Response({"message": "Not enough co-occurring product pairs for analysis."}, status=200)

        pair_counts = Counter(pairs)
        most_common_pairs = dict(pair_counts.most_common(10))

        # Affinity scores
        affinity_scores = {}
        for (prod_a, prod_b), count in pair_counts.items():
            a_orders = binary_basket[prod_a].sum()
            b_orders = binary_basket[prod_b].sum()
            denominator = a_orders + b_orders - count
            affinity = count / denominator if denominator else 0
            affinity_scores[(prod_a, prod_b)] = round(affinity, 3)

        # Central products
        product_links = Counter()
        for (a, b), count in pair_counts.items():
            product_links[a] += count
            product_links[b] += count
        central_products = dict(product_links.most_common(10))

    except Exception as e:
        return Response({"error": f"Failed to process correlation analysis: {str(e)}"}, status=500)

    return Response({
        "most_common_product_pairs": {f"{a} & {b}": c for (a, b), c in most_common_pairs.items()},
        "product_correlation_matrix": correlation_matrix.to_dict(),
        "product_affinity_scores": {f"{a} & {b}": s for (a, b), s in sorted(affinity_scores.items(), key=lambda x: -x[1])[:10]},
        "top_correlated_products_by_centrality": central_products
    })

@api_view(["GET"])
def product_trend_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    granularity = request.GET.get("interval", "M")  # D, W, M
    store_filter = request.GET.get("store")
    sender_filter = request.GET.get("sender")
    product_filter = request.GET.get("product")
    try:
        top_n = int(request.GET.get("top", 5))
    except ValueError:
        return Response({"error": "Invalid top N value, must be an integer."}, status=400)

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    try:
        df = filter_by_date(df, start_date, end_date)
        if store_filter:
            df = df[df["Store Name"].str.lower() == store_filter.lower()]
        if sender_filter:
            df = df[df["Sender Name"].str.lower() == sender_filter.lower()]
        if product_filter:
            df = df[df["Product Description"].str.lower().str.contains(product_filter.lower())]
    except Exception as e:
        return Response({"error": f"Error during filtering: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No data found for selected filters."}, status=200)

    try:
        df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        df = df.dropna(subset=["Created Date"])
        df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce").fillna(0)
        df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"], errors="coerce").fillna(0)
        df["Order Number"] = df["Order Number"].astype(str)

        df["Period"] = df["Created Date"].dt.to_period(granularity).astype(str)

        revenue_trend = df.groupby(["Period", "Product Description"])["Net Extended Line Cost"].sum().reset_index()
        quantity_trend = df.groupby(["Period", "Product Description"])["Requested Qty"].sum().reset_index()
        freq_trend = df.groupby(["Period", "Product Description"])["Order Number"].nunique().reset_index()

        revenue_pivot = revenue_trend.pivot(index="Period", columns="Product Description", values="Net Extended Line Cost").fillna(0)
        quantity_pivot = quantity_trend.pivot(index="Period", columns="Product Description", values="Requested Qty").fillna(0)
        freq_pivot = freq_trend.pivot(index="Period", columns="Product Description", values="Order Number").fillna(0)

        product_totals = df.groupby("Product Description")["Net Extended Line Cost"].sum().sort_values(ascending=False)
        top_products = product_totals.head(top_n).index.tolist()

        if not top_products:
            return Response({"message": "No product trends available for top products."}, status=200)

        recent_periods = sorted(df["Period"].unique())[-3:]
        trend_summary = {}
        for product in top_products:
            try:
                product_series = revenue_pivot[product].reindex(recent_periods, fill_value=0)
                direction = "increasing" if product_series.iloc[-1] > product_series.iloc[0] else "declining"
                trend_summary[product] = {
                    "first_period": recent_periods[0],
                    "last_period": recent_periods[-1],
                    "change": round(product_series.iloc[-1] - product_series.iloc[0], 2),
                    "direction": direction
                }
            except Exception:
                trend_summary[product] = {
                    "first_period": None,
                    "last_period": None,
                    "change": 0,
                    "direction": "flat"
                }

    except Exception as e:
        return Response({"error": f"Failed to compute product trends: {str(e)}"}, status=500)

    return Response({
        "revenue_trend": revenue_pivot[top_products].round(2).to_dict(orient="index"),
        "quantity_trend": quantity_pivot[top_products].round(2).to_dict(orient="index"),
        "frequency_trend": freq_pivot[top_products].astype(int).to_dict(orient="index"),
        "top_products": product_totals.head(top_n).round(2).to_dict(),
        "trend_summary": trend_summary
    })


@api_view(["GET"])
def order_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    granularity = request.GET.get("interval", "M")  # 'D', 'W', 'M'
    store_filter = request.GET.get("store")
    sender_filter = request.GET.get("sender")
    product_filter = request.GET.get("product")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    try:
        df = filter_by_date(df, start_date, end_date)

        if store_filter:
            df = df[df["Store Name"].str.lower() == store_filter.lower()]

        if sender_filter:
            df = df[df["Sender Name"].str.lower() == sender_filter.lower()]

        if product_filter:
            df = df[df["Product Description"].str.lower().str.contains(product_filter.lower())]

        if df.empty:
            return Response({"message": "No data available for the selected filters."}, status=200)

        # Ensure datetime
        df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        df = df.dropna(subset=["Created Date"])

        # Basic metrics
        total_orders = df["Order Number"].nunique()
        total_value = pd.to_numeric(df["Net Extended Line Cost"], errors="coerce").sum()
        unique_products = df["Product Description"].nunique()

        avg_order_value = (
            df.groupby("Order Number")["Net Extended Line Cost"]
            .sum(min_count=1)
            .mean()
        )
        items_per_order = (
            df.groupby("Order Number")["Requested Qty"]
            .sum(min_count=1)
            .mean()
        )

        # Order volume trend
        df["Period"] = df["Created Date"].dt.to_period(granularity).astype(str)
        order_trend = df.groupby("Period")["Order Number"].nunique().to_dict()

        # Top customers and stores
        top_customers = (
            df.groupby("Sender Name")["Order Number"]
            .nunique()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        )

        top_stores = (
            df.groupby("Store Name")["Order Number"]
            .nunique()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        )

        # Fulfillment metrics
        df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors="coerce")
        df_fulfilled = df.dropna(subset=["Delivery Date"])
        df_fulfilled["Fulfillment Days"] = (df_fulfilled["Delivery Date"] - df_fulfilled["Created Date"]).dt.days

        fulfillment_stats = {
            "average_days": round(df_fulfilled["Fulfillment Days"].mean(), 2)
            if not df_fulfilled.empty else None,
            "max_days": int(df_fulfilled["Fulfillment Days"].max())
            if not df_fulfilled["Fulfillment Days"].empty else None,
            "min_days": int(df_fulfilled["Fulfillment Days"].min())
            if not df_fulfilled["Fulfillment Days"].empty else None,
        }

    except Exception as e:
        return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

    return Response({
        "total_orders": total_orders,
        "unique_products_ordered": unique_products,
        "total_order_value": round(total_value, 2),
        "average_order_value": round(avg_order_value or 0, 2),
        "average_items_per_order": round(items_per_order or 0, 2),
        "order_volume_trend": order_trend,
        "top_customers_by_orders": top_customers,
        "top_stores_by_orders": top_stores,
        "fulfillment_stats": fulfillment_stats
    })

@api_view(["GET"])
def order_fulfillment_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    sla_param = request.GET.get("sla", 5)

    try:
        sla_days = int(sla_param)
    except (TypeError, ValueError):
        return Response({"error": "Invalid SLA value. It must be an integer."}, status=400)

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    try:
        df = filter_by_date(df, start_date, end_date)

        if df.empty:
            return Response({"message": "No data found in the selected period."}, status=200)

        df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors="coerce")
        df = df.dropna(subset=["Created Date"])

        total_orders = df["Order Number"].nunique()

        # Canceled orders
        canceled_orders_df = df[df["Delivery Date"].isna()]
        canceled_orders = canceled_orders_df["Order Number"].nunique()
        cancellation_rate = round((canceled_orders / total_orders) * 100, 2) if total_orders else 0

        # Fulfilled orders
        fulfilled_df = df.dropna(subset=["Delivery Date"]).copy()
        fulfilled_df["Fulfillment Days"] = (fulfilled_df["Delivery Date"] - fulfilled_df["Created Date"]).dt.days

        if fulfilled_df.empty:
            return Response({"message": "No fulfilled orders in this period."}, status=200)

        # Fulfillment stats
        stats = fulfilled_df["Fulfillment Days"].describe().round(2).to_dict()
        stats["std"] = round(fulfilled_df["Fulfillment Days"].std(), 2)

        # SLA compliance
        within_sla = (fulfilled_df["Fulfillment Days"] <= sla_days).sum()
        total_fulfilled_orders = fulfilled_df["Order Number"].nunique()
        sla_pct = round((within_sla / total_fulfilled_orders) * 100, 2) if total_fulfilled_orders else 0

        # Delivery efficiency
        delivery_rate = round((total_fulfilled_orders / total_orders) * 100, 2) if total_orders else 0
        delivery_efficiency_score = round((delivery_rate * sla_pct) / 100, 2)

        # Performance by store and sender
        by_store = fulfilled_df.groupby("Store Name")["Fulfillment Days"].mean().round(2).sort_values().to_dict()
        by_sender = fulfilled_df.groupby("Sender Name")["Fulfillment Days"].mean().round(2).sort_values().to_dict()

        # Top delays
        delayed = fulfilled_df[fulfilled_df["Fulfillment Days"] > sla_days]
        top_delays = delayed.sort_values("Fulfillment Days", ascending=False)
        top_delays = top_delays[["Order Number", "Store Name", "Sender Name", "Fulfillment Days"]].head(5).to_dict(orient="records")

        # Distribution
        dist = fulfilled_df["Fulfillment Days"].value_counts().sort_index().to_dict()

        return Response({
            "fulfillment_statistics": stats,
            "percent_within_sla": sla_pct,
            "delivery_rate": delivery_rate,
            "delivery_efficiency_score": delivery_efficiency_score,
            "cancellation_rate": cancellation_rate,
            "fulfillment_distribution": dist,
            "top_delayed_orders": top_delays,
            "average_fulfillment_by_store": by_store,
            "average_fulfillment_by_sender": by_sender
        })

    except Exception as e:
        return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

@api_view(["GET"])
def order_calculation_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    threshold_param = request.GET.get("threshold", 1000)

    try:
        high_value_threshold = float(threshold_param)
    except (TypeError, ValueError):
        return Response({"error": "Invalid threshold. It must be a numeric value."}, status=400)

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    try:
        df = filter_by_date(df, start_date, end_date)

        if df.empty:
            return Response({"message": "No order data found in this period."}, status=200)

        # Ensure datetime conversion
        df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        df = df.dropna(subset=["Created Date"])

        # Aggregate order-level data
        order_value = df.groupby("Order Number")["Net Extended Line Cost"].sum()
        order_items = df.groupby("Order Number")["Requested Qty"].sum()
        order_products = df.groupby("Order Number")["Product Description"].nunique()

        total_orders = order_value.count()
        total_lines = df.shape[0]
        avg_order_value = round(order_value.mean(), 2) if not order_value.empty else 0
        avg_items = round(order_items.mean(), 2) if not order_items.empty else 0
        avg_products = round(order_products.mean(), 2) if not order_products.empty else 0

        # Identify high-value and low-value orders
        high_value_orders = order_value[order_value > high_value_threshold]
        low_value_orders = order_value[order_value <= high_value_threshold]

        high_value_sample = high_value_orders.sort_values(ascending=False).head(5).round(2).to_dict()
        low_value_sample = low_value_orders.sort_values().head(5).round(2).to_dict()

        high_pct = round((len(high_value_orders) / total_orders) * 100, 2) if total_orders else 0
        low_pct = round((len(low_value_orders) / total_orders) * 100, 2) if total_orders else 0

        # Order frequency trend
        df["Order Date"] = df["Created Date"].dt.to_period("M").astype(str)
        order_freq = df.groupby("Order Date")["Order Number"].nunique().to_dict()

        # Order value distribution
        bins = [0, 250, 500, 1000, 2000, float("inf")]
        labels = ["<250", "250-500", "500-1k", "1k-2k", ">2k"]
        df["Order Value"] = df.groupby("Order Number")["Net Extended Line Cost"].transform("sum")
        df["Value Range"] = pd.cut(df["Order Value"], bins=bins, labels=labels)
        distribution = df["Value Range"].value_counts().sort_index().to_dict()

        return Response({
            "order_summary": {
                "total_orders": total_orders,
                "total_order_lines": total_lines,
                "avg_order_value": avg_order_value,
                "avg_items_per_order": avg_items,
                "avg_product_types_per_order": avg_products
            },
            "high_value_orders": {
                "count": len(high_value_orders),
                "percentage": high_pct,
                "sample": high_value_sample
            },
            "low_value_orders": {
                "count": len(low_value_orders),
                "percentage": low_pct,
                "sample": low_value_sample
            },
            "order_value_distribution": distribution,
            "order_frequency_trend": order_freq
        })

    except Exception as e:
        return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

@api_view(["GET"])
def customer_segmentation_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    today = datetime.today().date()

    try:
        df = load_data()
        df = filter_by_date(df, start_date, end_date)
        df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
        df.dropna(subset=["Created Date", "Sender Name"], inplace=True)
    except Exception as e:
        return Response({"error": f"Data error: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No transaction data found for the selected period."})

    # ===== RFM Calculation =====
    rfm = df.groupby("Sender Name").agg({
        "Created Date": lambda x: (today - x.max().date()).days,   # Recency
        "Order Number": "nunique",                                 # Frequency
        "Net Extended Line Cost": "sum"                            # Monetary
    }).reset_index()

    rfm.columns = ["Customer", "Recency", "Frequency", "Monetary"]
    rfm["Segment"] = pd.qcut(rfm["Monetary"], 4, labels=["Low", "Mid-Low", "Mid-High", "High"])

    # ===== Revenue Over Time by Customer =====
    df["Period"] = df["Created Date"].dt.to_period("M").astype(str)
    revenue_time = df.groupby(["Period", "Sender Name"])["Net Extended Line Cost"].sum().reset_index()
    revenue_pivot = revenue_time.pivot(index="Period", columns="Sender Name", values="Net Extended Line Cost").fillna(0).round(2)
    revenue_over_time = revenue_pivot.to_dict(orient="index")

    # ===== Top Growing Customers by Revenue (Last 2 Periods) =====
    if len(revenue_pivot.index) >= 2:
        latest_two = revenue_pivot.iloc[-2:]
        revenue_diff = latest_two.diff().iloc[-1].sort_values(ascending=False)
        top_growing_customers = revenue_diff.head(5).round(2).to_dict()
    else:
        top_growing_customers = {}

    # ===== Customer Churn Indication =====
    churn_threshold = 30  # days without purchase
    churned_customers = rfm[rfm["Recency"] > churn_threshold].sort_values("Recency", ascending=False)
    churn_list = churned_customers[["Customer", "Recency", "Monetary"]].head(10).round(2).to_dict(orient="records")

    return Response({
        "customer_rfm": rfm.round(2).to_dict(orient="records"),
        "summary": {
            "total_customers": rfm.shape[0],
            "high_value_customers": int((rfm["Segment"] == "High").sum()),
            "low_value_customers": int((rfm["Segment"] == "Low").sum()),
        },
        "revenue_over_time": revenue_over_time,
        "top_growing_customers": top_growing_customers,
        "churned_customers": churn_list
    })


@api_view(["GET"])
def customer_purchase_pattern_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_data()
        df = filter_by_date(df, start_date, end_date)
        df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
        df.dropna(subset=["Created Date", "Sender Name", "Order Number"], inplace=True)
    except Exception as e:
        return Response({"error": f"Data error: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No transaction data found for the selected period."})

    today = datetime.today().date()
    df["Date"] = df["Created Date"].dt.date
    df["Weekday"] = df["Created Date"].dt.day_name()
    df["Hour"] = df["Created Date"].dt.hour

    # 1. Aggregate Customer Summary
    customer_summary = df.groupby("Sender Name").agg(
        total_orders=("Order Number", "nunique"),
        total_revenue=("Net Extended Line Cost", "sum"),
        total_quantity=("Requested Qty", "sum"),
        first_purchase=("Date", "min"),
        last_purchase=("Date", "max"),
        distinct_products=("Product Description", "nunique")
    ).reset_index()

    customer_summary["days_since_last_purchase"] = customer_summary["last_purchase"].apply(lambda x: (today - x).days)
    customer_summary["avg_order_value"] = (customer_summary["total_revenue"] / customer_summary["total_orders"]).round(2)
    customer_summary["avg_items_per_order"] = (customer_summary["total_quantity"] / customer_summary["total_orders"]).round(2)

    # 2. Purchase Frequency Patterns
    order_dates = df.groupby(["Sender Name", "Order Number"])["Date"].min().reset_index()
    order_diffs = order_dates.groupby("Sender Name")["Date"].apply(lambda x: x.sort_values().diff().dt.days.dropna())
    avg_days_between_orders = order_diffs.groupby(level=0).mean().round(2)
    repeat_rate = order_dates.groupby("Sender Name")["Order Number"].count().apply(lambda x: 1 if x > 1 else 0)

    customer_summary = customer_summary.set_index("Sender Name")
    customer_summary["avg_days_between_orders"] = avg_days_between_orders
    customer_summary["is_repeater"] = repeat_rate

    # 3. Product Preferences
    top_products = (
        df.groupby(["Sender Name", "Product Description"])["Requested Qty"]
        .sum().reset_index()
        .sort_values(["Sender Name", "Requested Qty"], ascending=[True, False])
    )
    top_products = top_products.groupby("Sender Name").head(3).groupby("Sender Name")["Product Description"].apply(list)
    customer_summary["top_products"] = top_products

    # 4. Time-Based Patterns
    weekday_pref = df.groupby(["Sender Name", "Weekday"])["Order Number"].nunique().reset_index()
    weekday_pref = weekday_pref.sort_values(["Sender Name", "Order Number"], ascending=[True, False])
    top_weekday = weekday_pref.groupby("Sender Name").first().reset_index()
    customer_summary["top_order_day"] = top_weekday.set_index("Sender Name")["Weekday"]

    hour_pref = df.groupby(["Sender Name", "Hour"])["Order Number"].nunique().reset_index()
    hour_pref = hour_pref.sort_values(["Sender Name", "Order Number"], ascending=[True, False])
    top_hour = hour_pref.groupby("Sender Name").first().reset_index()
    customer_summary["top_order_hour"] = top_hour.set_index("Sender Name")["Hour"]

    # 5. Customer Segments
    def segment(row):
        if row["total_orders"] == 1:
            return "New"
        elif row["avg_days_between_orders"] and row["avg_days_between_orders"] < 14:
            return "Frequent"
        elif row["is_repeater"]:
            return "Returning"
        return "One-time"

    customer_summary["segment"] = customer_summary.apply(segment, axis=1)

    # 6. Order Timeline per Customer
    timeline = df.groupby(["Sender Name", "Date"])["Order Number"].nunique().reset_index()
    customer_timeline = (
        timeline.groupby("Sender Name")
        .apply(lambda x: x.drop(columns="Sender Name").sort_values("Date").to_dict(orient="records"))
        .to_dict()
    )

    # Final output
    result = customer_summary.reset_index().round(2).to_dict(orient="records")

    return Response({
        "customer_purchase_patterns": result,
        "customer_order_timeline": customer_timeline,
        "summary": {
            "total_customers": len(customer_summary),
            "frequent_customers": int((customer_summary["segment"] == "Frequent").sum()),
            "returning_customers": int((customer_summary["segment"] == "Returning").sum()),
            "new_customers": int((customer_summary["segment"] == "New").sum())
        }
    })

