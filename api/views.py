from collections import Counter
from datetime import datetime, timedelta
from itertools import combinations
import threading
from django.shortcuts import render
import pandas as pd
from rest_framework.decorators import api_view
from rest_framework.response import Response
import openai
import os
import numpy as np
from rest_framework import status
from dateutil.relativedelta import relativedelta
from django.utils.dateparse import parse_date
from django.http import JsonResponse
import sys
from django.core.cache import cache
import hashlib
import json
from scipy.spatial.distance import cosine
from scipy.stats import pearsonr
from .utils import compute_purchase_patterns, compute_rfm, compute_clv, compute_churn, load_dataset
def python_version_view(request):
    return JsonResponse({"python_version": sys.version})
EXCEL_PATH = os.path.join(os.path.dirname(__file__), 'data/data_adjusted.xlsx')
SHEET_NAME = 'salesData'
SHEET_NAME = 'invoiceData'
# def load_data():
#     df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
#     df['Created Date'] = pd.to_datetime(df['Created Date'])
#     return df

# def load_data():
#     # Load both sheets
#     sales_df = pd.read_excel(EXCEL_PATH, sheet_name="salesData")
#     invoice_df = pd.read_excel(EXCEL_PATH, sheet_name="invoiceData")

#     # Convert date columns safely
#     for df in [sales_df, invoice_df]:
#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
#         if "Date Delivered" in df.columns:
#             df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors='coerce')
#         else:
#             df["Date Delivered"] = pd.NaT

#     # Columns you care about
#     common_cols = [
#         "Sender Code", "Sender Name", "Receiver Code", "Receiver Name",
#         "Store Code", "Store Name", "Order Number", "Barcode", "Product Code",
#         "Product Description", "Requested Qty", "Cost Price", "Net Extended Line Cost",
#         "Created Date", "Delivery date"
#     ]

#     sales_df = sales_df[[c for c in common_cols if c in sales_df.columns]].copy()
#     invoice_df = invoice_df[[c for c in common_cols if c in invoice_df.columns]].copy()

#     # Add source column
#     sales_df["Source"] = "Order"
#     invoice_df["Source"] = "Invoice"

#     # Combine both for any global analysis
#     combined_df = pd.concat([sales_df, invoice_df], ignore_index=True)
#     combined_df.dropna(subset=["Created Date", "Sender Name"], inplace=True)

#     return combined_df, sales_df, invoice_df

def load_data():
    # Load both sheets
    sales_df = pd.read_excel(EXCEL_PATH, sheet_name="salesData")
    invoice_df = pd.read_excel(EXCEL_PATH, sheet_name="invoiceData")

    # --- Normalize column names ---
    sales_df.columns = sales_df.columns.str.strip()
    invoice_df.columns = invoice_df.columns.str.strip()

    rename_map = {
        "Date Created": "Created Date",
        "Order reference": "Order Number",
        "Delivery date": "Delivery Date",
    }

    sales_df.rename(columns=rename_map, inplace=True)
    invoice_df.rename(columns=rename_map, inplace=True)

    # Convert date columns safely
    for df in [sales_df, invoice_df]:
        if "Created Date" in df.columns:
            df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
        if "Delivery Date" in df.columns:
            df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors='coerce')

    # Columns you care about
    common_cols = [
        "Sender", "Sender Name", "Receiver Code", "Receiver Name",
        "Store Code", "Store Name", "Order Number", "Barcode", "Product Code",
        "Product Description", "Requested Qty", "Cost Price", "Net Extended Line Cost",
        "Created Date", "Delivery Date"
    ]

    sales_df = sales_df[[c for c in common_cols if c in sales_df.columns]].copy()
    invoice_df = invoice_df[[c for c in common_cols if c in invoice_df.columns]].copy()

    # Add source
    sales_df["Source"] = "Order"
    invoice_df["Source"] = "Invoice"

    combined_df = pd.concat([sales_df, invoice_df], ignore_index=True)
    combined_df.dropna(subset=["Created Date", "Sender Name"], inplace=True)

    return combined_df, sales_df, invoice_df


# # Helper to generate OpenAI insights
def generate_insight(prompt_prefix, data_summary, role="You are a business analyst explaining the data to non-technical executives.", model="gpt-4"):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise Exception("OPENAI_API_KEY is not set in the environment.")

    client = openai.OpenAI(api_key=api_key)

    context = f"""{role}

    Data Summary:
    {data_summary}

    {prompt_prefix}
    """

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": role},
            {"role": "user", "content": context}
        ],
        temperature=0.7,
        max_tokens=1000
    )
    return response.choices[0].message.content

def get_insight_and_forecast(data_summary, start_date, end_date, period):
    insight_prompt = "Explain these analytics results in business terms. What trends stand out, and what should management know. the currency is ZAR?"
    forecast_prompt = f"Based on the trends from {start_date} to {end_date}, forecast what the next {period} period might look like. Be clear and business-oriented. the currency is ZAR"

    insight = generate_insight(insight_prompt, data_summary)
    forecast = generate_insight(forecast_prompt, data_summary, role="You are a business forecaster.")

    return insight, forecast

def get_prompts_for_view(view_name, start_date, end_date, period, granularity=None, sla_param=None, threshold_param=None):
    """
    Returns (insight_prompt, forecast_prompt) based on the view name and date range.
    """
    if view_name == "sales_trend_analytics":
        insight_prompt = (
            "Analyze this sales trend data in business terms. "
            "Identify growth or decline patterns, best time periods, and significant product trends. "
            "The currency is ZAR."
        )
        forecast_prompt = (
            f"Based on the sales trend from {start_date} to {end_date}, "
            f"forecast what the next {period} might look like. "
            "Focus on expected patterns over time. Currency is ZAR."
        )
    elif view_name == "sales_analytics":
        insight_prompt = (
            "Explain these sales metrics in business terms. "
            "Highlight total sales, order value trends, top products, and customer contributions. "
            "The currency is ZAR."
        )
        forecast_prompt = (
            f"Forecast sales for the next {period} period based on the current performance summary. "
            "Be concise and business-friendly. Currency is ZAR."
        )
    elif view_name == "profit_margin_analytics":
        insight_prompt = (
            "Explain the profit margin analytics in business terms. "
            "Highlight profit trends, margin shifts, cost efficiency, and how they affect profitability. "
            "Point out the best-performing time periods. Currency is ZAR."
        )
        forecast_prompt = (
            f"Based on the profit margin data from {start_date} to {end_date}, "
            f"forecast profit and margin expectations for the next {period} period. "
            "Be business-focused. Currency is ZAR."
        )
    elif view_name == "cost_analysis":
            insight_prompt = (
                "Analyze the cost trends across stores and products for the given time period. "
                "Highlight where costs are highest, growth patterns in cost, and any notable concentration of expenses. "
                "The currency is ZAR."
            )
            forecast_prompt = (
                f"Based on the cost data from {start_date} to {end_date}, "
                f"forecast what the total and category-level costs might look like in the next {period} period. "
                "Provide actionable insights. Currency is ZAR."
            )
    elif view_name == "sales_summary":
        insight_prompt = (
            "Summarize this sales summary data in business terms. "
            "Highlight order volumes, average sales values, and top-selling products. The currency is ZAR."
        )
        forecast_prompt = (
            f"Based on sales summary trends from {start_date} to {end_date}, "
            f"forecast key sales figures and product performance for the next {period}. Currency is ZAR."
        )
    elif view_name == "transaction_summary":
        insight_prompt = (
            "Analyze the transaction summary data for patterns in quantity, order value, and store/product distribution. "
            "Highlight any shifts or notable trends in the current period vs. previous. Currency is ZAR."
        )
        forecast_prompt = (
            f"Based on transactions from {start_date} to {end_date}, forecast what the next similar period might look like "
            "in terms of transaction value, volume, and order behavior. Currency is ZAR."
        )
    elif view_name == "transaction_entities_analysis":
        insight_prompt = (
            "Analyze the transaction data by store and sender. "
            "Identify the top-performing stores, customers, and products in terms of revenue. "
            "Include monthly trends and revenue contribution percentages. Currency is ZAR."
        )
        forecast_prompt = (
            f"Forecast which entities (stores or customers) might dominate transactions in the next {period} "
            f"based on trends between {start_date} and {end_date}. Currency is ZAR."
        )
    elif view_name == "transaction_timing_analysis":
        insight_prompt = (
            "Analyze this transaction timing data to identify patterns in when transactions are most frequent. "
            "Highlight the most active weekdays and hours, and summarize fulfillment time trends."
        )
        forecast_prompt = (
            f"Based on transaction timing data from {start_date} to {end_date}, "
            f"forecast potential fulfillment delays and peak order times for the next {period}. "
            "Mention expected fast/slow delivery periods."
        )
    elif view_name == "product_demand_analysis":
        insight_prompt = (
            "Analyze this product demand data. Identify the most demanded products, "
            "rising trends, velocity of demand, and seasonality patterns (by month and weekday)."
        )
        forecast_prompt = (
            f"Based on product demand from {start_date} to {end_date}, "
            f"forecast product demand trends for the next {period}. Highlight expected high and low demand products."
        )

    elif view_name == "product_revenue_analysis":
        insight_prompt = (
            "Analyze this product revenue data. Highlight top revenue-generating products, revenue yield per product, "
            "and store-level product performance. Identify seasonality trends by month and weekday."
        )
        forecast_prompt = (
            f"Based on product revenue from {start_date} to {end_date}, forecast revenue trends for the next {period}. "
            f"Identify which products or store segments are expected to grow or decline."
        )
    elif view_name == "product_correlation_analysis":
        insight_prompt = (
            "Based on product correlation data, analyze which product combinations frequently appear in the same orders. "
            "Highlight affinity scores, most central products in the network, and the most common co-purchased product pairs."
        )
        forecast_prompt = (
            f"Using product correlation patterns from {start_date} to {end_date}, predict which product affinities are likely to strengthen. "
            f"Forecast emerging co-purchase patterns and central products expected to drive bundled sales."
        )
    elif view_name == "product_trend_analysis":
        insight_prompt = (
            "Analyze revenue, quantity, and frequency trends of top products over the selected period. "
            "Summarize which products are gaining or losing momentum and highlight any notable shifts in product performance."
        )
        forecast_prompt = (
            f"Based on recent trends in revenue, quantity, and frequency for top products between {start_date} and {end_date}, "
            f"forecast which products are expected to increase in popularity or decline in the next few {granularity}-based periods."
        )
    elif view_name == "order_analysis":
        insight_prompt = (
            "Summarize order patterns and behaviors over the selected period. "
            "Identify customer and store order dynamics, fulfillment delays, and order volume fluctuations."
        )
        forecast_prompt = (
            f"Based on order trends, value, and fulfillment metrics from {start_date} to {end_date}, "
            f"forecast expected order patterns and fulfillment efficiency for the next {granularity}-based period."
        )
    elif view_name == "order_fulfillment_analysis":
        insight_prompt = (
            f"Analyze order fulfillment performance between {start_date} and {end_date}, "
            f"including SLA compliance (within {sla_param or 5} days), cancellation rate, delivery efficiency, and fulfillment time distribution. "
            f"Highlight key bottlenecks or outstanding performers by store or sender."
        )
        forecast_prompt = (
            f"Based on the order fulfillment trends and SLA compliance from {start_date} to {end_date}, "
            f"predict expected fulfillment performance and SLA adherence for the next similar period."
        )
    elif view_name == "order_calculation_analysis":
        insight_prompt = (
            f"Analyze order-level behavior between {start_date} and {end_date}, focusing on average value, item count, and product diversity. "
            f"Highlight the proportion and characteristics of orders above and below the {threshold_param} threshold, and give insights into value distribution."
        )
        forecast_prompt = (
            f"Based on the current distribution of order values and item patterns from {start_date} to {end_date}, "
            f"predict expected order trends and high-value order dynamics for the next similar period."
        )
    elif view_name == "customer_segmentation_analysis":
        insight_prompt = (
            f"Analyze customer behavior from {start_date} to {end_date} using RFM (Recency, Frequency, Monetary) segmentation. "
            f"Highlight customer distribution, top-value segments, recent revenue growth, and potential churn."
        )
        forecast_prompt = (
            f"Based on customer RFM behavior from {start_date} to {end_date}, forecast future customer segments, revenue growth, and likely churn rates "
            f"in the next equivalent period."
        )
    elif view_name == "customer_purchase_pattern_analysis":
        insight_prompt = (
            f"Analyze detailed customer purchasing behavior from {start_date} to {end_date}, including order frequency, "
            f"preferred days/hours, top products, repeat patterns, and segments (New, Returning, Frequent). "
            f"Highlight the most engaged and predictable customer profiles."
        )
        forecast_prompt = (
            f"Based on the purchasing behavior from {start_date} to {end_date}, forecast the expected number of repeat, frequent, and new customers "
            f"and likely preferred products and ordering times in the next period."
        )
    elif view_name == "invoice_trend_and_conversion":
        insight_prompt = (
            f"Analyze invoice trends and order-to-invoice conversion performance between {start_date} and {end_date}. "
            f"Highlight the conversion rate of orders to invoices, key fluctuations over time, and any seasonal or store-level differences. "
            f"Identify which stores or senders have the most efficient order-to-invoice processes and discuss possible causes of delays."
        )
        forecast_prompt = (
            f"Based on invoice trend and conversion data from {start_date} to {end_date}, "
            f"forecast the expected order-to-invoice conversion rate and invoicing volume for the next {period}. "
            f"Identify stores or senders likely to improve or decline in efficiency."
        )

    elif view_name == "product_performance_analysis":
        insight_prompt = (
            f"Analyze product performance between {start_date} and {end_date}. "
            f"Highlight top and bottom performing products by profit, revenue, and demand. "
            f"Discuss average profit margin, return and refund rates, and identify trends in demand across months. "
            f"If available, include comments on inventory turnover and stock holding efficiency. "
            f"Summarize store or product categories that are driving or dragging profitability. "
            f"The currency is ZAR."
        )
        forecast_prompt = (
            f"Based on product performance trends from {start_date} to {end_date}, "
            f"forecast demand, profitability, and inventory turnover expectations for the next {period}. "
            f"Identify which products or categories are likely to gain or lose momentum."
        )

    else:
        # Default fallback for unrecognized views
        insight_prompt = (
            "Explain the analytics results in simple business terms. "
            "Highlight any noticeable trends or performance indicators. Currency is ZAR."
        )
        forecast_prompt = (
            f"Based on trends from {start_date} to {end_date}, forecast what the next {period} might look like. "
            "Currency is ZAR."
        )

    return insight_prompt, forecast_prompt

def generate_ai_cache_key(summary_payload, start_date, end_date, period):
    def convert(obj):
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, (dict, list)):
            return json.loads(json.dumps(obj, default=convert))
        return obj

    key_data = json.dumps({
        "summary": convert(summary_payload),
        "start_date": str(start_date),
        "end_date": str(end_date),
        "period": period,
    }, sort_keys=True)

    return "sales_ai:" + hashlib.md5(key_data.encode()).hexdigest()

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
        return "new product" if current > 0 else 0
    return round(((current - previous) / previous) * 100, 2)


def parse_float(value):
    try:
        return float(str(value).replace(",", ""))
    except:
        return 0.0

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

def generate_insight_and_forecast_background(data_summary, start_date, end_date, period, cache_key, view_name="default"):
    try:
        insight_prompt, forecast_prompt = get_prompts_for_view(view_name, start_date, end_date, period)

        insight = generate_insight(insight_prompt, data_summary)
        forecast = generate_insight(forecast_prompt, data_summary, role="You are a business forecaster.")

        cache.set(cache_key + ":insight", insight, timeout=3600)
        cache.set(cache_key + ":forecast", forecast, timeout=3600)
        cache.set(cache_key + ":status", {"insight": "completed", "forecast": "completed"}, timeout=3600)
    except Exception as e:
        cache.set(cache_key + ":insight", f"Insight generation failed: {str(e)}", timeout=3600)
        cache.set(cache_key + ":forecast", f"Forecast generation failed: {str(e)}", timeout=3600)
        cache.set(cache_key + ":status", {"insight": "failed", "forecast": "failed"}, timeout=3600)

@api_view(["GET"])
def get_sales_insight_result(request):
    cache_key = request.GET.get("key")
    if not cache_key:
        return Response({"error": "Missing ?key= query parameter"}, status=400)

    insight = cache.get(cache_key + ":insight", "Processing...")
    forecast = cache.get(cache_key + ":forecast", "Processing...")
    status_info = cache.get(cache_key + ":status", {"insight": "processing", "forecast": "processing"})

    return Response({
        "insight": insight,
        "forecast": forecast,
        "status": status_info
    })

'''SALES ANALYTICS'''

@api_view(["GET"])
def sales_analytics(request):
    try:
        # ===== INPUT VALIDATION =====
        start_date_str = request.GET.get("start_date")
        end_date_str = request.GET.get("end_date")
        period = request.GET.get("period", "monthly").lower()

        if not start_date_str or not end_date_str:
            return Response(
                {"error": "start_date and end_date are required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except:
            return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

        # ===== LOAD DATASET =====
        df = load_dataset()

        # ===== REQUIRED COLUMNS =====
        required_cols = [
            "main_date",
            "order_number",
            "net_extended_line_cost",
            "product_description",
            "store_gln",
            "store_name"
        ]
        for col in required_cols:
            if col not in df.columns:
                return Response({"error": f"Missing required column: {col}"}, status=500)

        # ===== FILTER DATE RANGE =====
        df = df[
            (df["main_date"].dt.date >= start_date)
            & (df["main_date"].dt.date <= end_date)
        ].copy()

        if df.empty:
            return Response({"error": "No data found for this period."}, status=404)

        # ===== BASIC METRICS =====
        total_sales = df["net_extended_line_cost"].sum()
        total_orders = df["order_number"].nunique()
        avg_order_value = total_sales / total_orders if total_orders else 0

        # ===== PERIOD GROUPING =====
        if period == "weekly":
            df["period"] = df["main_date"].dt.to_period("W").apply(lambda r: r.start_time)
            delta = timedelta(weeks=1)
        elif period == "monthly":
            df["period"] = df["main_date"].dt.to_period("M").apply(lambda r: r.start_time)
            delta = relativedelta(months=1)
        elif period == "yearly":
            df["period"] = df["main_date"].dt.to_period("Y").apply(lambda r: r.start_time)
            delta = relativedelta(years=1)
        else:
            return Response({"error": "Invalid period. Use weekly, monthly, or yearly."}, status=400)

        performance = (
            df.groupby("period")["net_extended_line_cost"]
            .sum().reset_index().sort_values("period")
        )

        # ===== GROWTH CALCULATION =====
        if len(performance) > 1:
            last = performance["net_extended_line_cost"].iloc[-1]
            prev = performance["net_extended_line_cost"].iloc[-2]
            growth = ((last - prev) / prev) * 100 if prev != 0 else 0
        else:
            growth = 0

        # ===== BEST PRODUCTS =====
        best_products = (
            df.groupby("product_description")["net_extended_line_cost"]
            .sum().reset_index()
            .sort_values("net_extended_line_cost", ascending=False)
            .head(5)
            .rename(columns={"net_extended_line_cost": "sales"})
            .to_dict(orient="records")
        )

        # ===== TOP STORES =====
        top_stores = (
            df.groupby(["store_name", "store_gln"])["net_extended_line_cost"]
            .sum().reset_index()
            .sort_values("net_extended_line_cost", ascending=False)
            .head(5)
            .rename(columns={"net_extended_line_cost": "sales"})
            .to_dict(orient="records")
        )

        # ===== COMPILE SUMMARY PAYLOAD =====
        summary_payload = {
            "total_sales_value": round(total_sales, 2),
            "total_orders": int(total_orders),
            "avg_order_value": round(avg_order_value, 2),
            "sales_growth_percent": round(growth, 2),
            "sales_performance_breakdown": performance.to_dict(orient="records"),
            "top_products": best_products,
            "customer_value": top_stores,  # B2B: store-level customers
        }

        # ====== CACHE KEY + AI BACKGROUND GENERATION ======
        cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, period)

        # Initialize cache status for async AI processing
        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        # Background AI generation thread
        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_payload, start_date, end_date, period, cache_key, "sales_analytics"),
        ).start()

        # ===== FINAL RESPONSE =====
        return Response({
            **summary_payload,
            "ai_status": "processing",
            "data_key": cache_key,
        }, status=200)

    except Exception as e:
        return Response({"error": f"Failed to compute analytics: {str(e)}"}, status=500)

@api_view(["GET"])
def sales_trend_analytics(request):
    try:
        # ====== INPUTS ======
        period = request.GET.get("period", "monthly").lower()
        start_date_str = request.GET.get("start_date")
        end_date_str = request.GET.get("end_date")

        # ====== LOAD DATA ======
        df = load_dataset()
        if df.empty:
            return Response({"error": "Dataset is empty or not loaded correctly."}, status=500)

        # ====== ENSURE REQUIRED COLUMNS EXIST ======
        # All columns are lowercase and underscore-separated
        numeric_col = "net_extended_line_cost"
        region_col = "region"
        channel_col = "channel"
        customer_col = "customer_type"
        product_col = "product_description"

        # Convert main date + cost to usable formats
        df["main_date"] = pd.to_datetime(df["main_date"], errors="coerce")
        df[numeric_col] = pd.to_numeric(df.get(numeric_col, 0), errors="coerce").fillna(0)

        today = df["main_date"].max().normalize()

        # ====== PERIOD DETERMINATION ======
        if start_date_str and end_date_str:
            try:
                start_current = pd.to_datetime(start_date_str)
                end_current = pd.to_datetime(end_date_str)
            except Exception:
                return Response({"error": "Invalid start_date or end_date. Use YYYY-MM-DD."}, status=400)

            duration = end_current - start_current
            start_previous = start_current - duration - timedelta(days=1)
            end_previous = start_current - timedelta(days=1)
        else:
            if period == "weekly":
                start_current = today - timedelta(days=today.weekday())
                end_current = start_current + timedelta(days=6)
                start_previous = start_current - timedelta(weeks=1)
                end_previous = start_current - timedelta(days=1)
                freq, label_format = "D", "%Y-%m-%d"
            elif period == "monthly":
                start_current = today.replace(day=1)
                end_current = (start_current + relativedelta(months=1)) - timedelta(days=1)
                start_previous = start_current - relativedelta(months=1)
                end_previous = start_current - timedelta(days=1)
                freq, label_format = "W-MON", "Week %W"
            elif period == "yearly":
                start_current = today.replace(month=1, day=1)
                end_current = today.replace(month=12, day=31)
                start_previous = start_current - relativedelta(years=1)
                end_previous = start_current - timedelta(days=1)
                freq, label_format = "M", "%B"
            else:
                return Response({"error": "Invalid or missing period."}, status=400)

        # Dynamically choose frequency if date range provided
        if start_date_str and end_date_str:
            days = (end_current - start_current).days
            if days <= 7:
                freq, label_format = "D", "%Y-%m-%d"
            elif days <= 31:
                freq, label_format = "W-MON", "Week %W"
            elif days <= 365:
                freq, label_format = "M", "%B"
            else:
                freq, label_format = "Q", "Q%q %Y"

        # ====== SLICE PERIODS ======
        df_current = df[(df["main_date"] >= start_current) & (df["main_date"] <= end_current)]
        df_previous = df[(df["main_date"] >= start_previous) & (df["main_date"] <= end_previous)]

        if df_current.empty:
            return Response({"error": "No sales records found for the current period."}, status=404)
        if df_previous.empty:
            return Response({"error": "No sales records found for the previous period."}, status=404)

        # ====== AGGREGATE SALES ======
        total_sales_current = df_current[numeric_col].sum()
        total_sales_previous = df_previous[numeric_col].sum()
        growth_percent = (
            ((total_sales_current - total_sales_previous) / total_sales_previous) * 100
            if total_sales_previous else (100.0 if total_sales_current else 0.0)
        )

        # ====== BREAKDOWN FUNCTION ======
        def breakdown(df_slice, freq, label_format):
            df_slice = df_slice.copy()
            df_slice["period"] = df_slice["main_date"].dt.to_period(freq).dt.start_time
            summary = df_slice.groupby("period")[numeric_col].sum().reset_index()
            summary.columns = ["period", "sales"]
            summary["label"] = summary["period"].dt.strftime(label_format)
            return summary.sort_values("period")

        current_breakdown = breakdown(df_current, freq, label_format)
        previous_breakdown = breakdown(df_previous, freq, label_format)

        # ====== GROWTH TREND ======
        growth_trend = current_breakdown.copy()
        growth_trend["growth_percent"] = growth_trend["sales"].pct_change().fillna(0) * 100
        growth_trend["growth_percent"] = growth_trend["growth_percent"].round(2)

        # ====== TOP CATEGORIES ======
        top_regions = df_current.groupby(region_col)[numeric_col].sum().nlargest(5).reset_index()
        top_channels = df_current.groupby(channel_col)[numeric_col].sum().nlargest(5).reset_index()
        top_customers = df_current.groupby(customer_col)[numeric_col].sum().nlargest(5).reset_index()
        top_products = df_current.groupby(product_col)[numeric_col].sum().nlargest(5).reset_index()

        # ====== BEST PERIOD ======
        best_time = current_breakdown.sort_values("sales", ascending=False).iloc[0].to_dict()

        # ====== QUARTERLY VIEW ======
        quarterly_breakdown = []
        if (period == "yearly") or ((end_current - start_current).days > 180):
            df_current["quarter"] = df_current["main_date"].dt.to_period("Q").dt.start_time
            quarterly_breakdown = (
                df_current.groupby("quarter")[numeric_col].sum().reset_index()
            )
            quarterly_breakdown["quarter"] = quarterly_breakdown["quarter"].dt.strftime("Q%q %Y")

        # ====== AI SUMMARY PAYLOAD ======
        summary_payload = {
            "period": period or "custom",
            "start_current": str(start_current.date()),
            "end_current": str(end_current.date()),
            "total_sales_current": round(total_sales_current, 2),
            "total_sales_previous": round(total_sales_previous, 2),
            "period_growth_percent": round(growth_percent, 2),
            "best_time_period": {
                "period": str(best_time.get("period")),
                "label": best_time.get("label"),
                "sales": round(best_time.get("sales", 0), 2),
            },
            "growth_trend": growth_trend.round(2).to_dict(orient="records"),
            "top_regions": top_regions.round(2).to_dict(orient="records"),
            "top_channels": top_channels.round(2).to_dict(orient="records"),
            "top_customers": top_customers.round(2).to_dict(orient="records"),
            "top_products": top_products.round(2).to_dict(orient="records"),
        }

        # ====== AI BACKGROUND JOB ======
        cache_key = generate_ai_cache_key(summary_payload, start_current, end_current, period)
        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_payload, start_current, end_current, period, cache_key, "sales_trend_analytics"),
        ).start()

        # ====== FINAL RESPONSE ======
        return Response({
            **summary_payload,
            "current_period_breakdown": current_breakdown.round(2).to_dict(orient="records"),
            "previous_period_breakdown": previous_breakdown.round(2).to_dict(orient="records"),
            "quarterly_breakdown": quarterly_breakdown if len(quarterly_breakdown) else [],
            "ai_status": "processing",
            "data_key": cache_key,
        })

    except Exception as e:
        return Response({"error": f"Failed to compute trend analysis: {str(e)}"}, status=500)

@api_view(["GET"])
def sales_summary(request):
    """
    Returns an enhanced sales overview for the selected period.
    Includes KPIs, product performance, regional breakdowns,
    and AI-powered insights (background processing).
    """
    try:
        df = load_dataset()  # uses your normalized helper
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    # ===== Detect main date column =====
    date_field = "order_date" if "order_date" in df.columns else "main_date"
    if date_field not in df.columns:
        return Response({"error": f"No valid date column found in dataset."}, status=500)

    # ===== Clean numeric fields =====
    df[date_field] = pd.to_datetime(df[date_field], errors="coerce")
    df = df.dropna(subset=[date_field])

    for col in ["net_extended_line_cost", "requested_qty", "profit_margin"]:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
                .fillna(0)
            )

    # ===== DATE FILTERING =====
    start_date_str = request.GET.get("start_date")
    end_date_str = request.GET.get("end_date")

    try:
        start_date = pd.to_datetime(start_date_str) if start_date_str else None
        end_date = pd.to_datetime(end_date_str) if end_date_str else None
        if start_date and end_date and start_date > end_date:
            return Response({"error": "start_date cannot be after end_date."}, status=400)
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    min_date = df[date_field].min()
    max_date = df[date_field].max()

    if (start_date and start_date > max_date) or (end_date and end_date < min_date):
        return Response({
            "error": "Provided date range is outside the available data range.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    if start_date:
        df = df[df[date_field] >= start_date]
    if end_date:
        df = df[df[date_field] <= end_date]

    if df.empty:
        return Response({"message": "No data available for the specified period."}, status=404)

    # ===== KEY METRICS =====
    total_orders = df["order_number"].nunique() if "order_number" in df.columns else 0
    total_revenue = df["net_extended_line_cost"].sum()
    avg_order_value = (
        df.groupby("order_number")["net_extended_line_cost"].sum().mean()
        if "order_number" in df.columns else 0
    )
    total_quantity = df["requested_qty"].sum() if "requested_qty" in df.columns else 0
    avg_profit_margin = df["profit_margin"].mean() if "profit_margin" in df.columns else 0
    total_customers = df["customer_id"].nunique() if "customer_id" in df.columns else 0

    # ===== TOP PRODUCTS =====
    top_products = (
        df.groupby("product_description")
        .agg(
            total_sales=("net_extended_line_cost", "sum"),
            total_quantity=("requested_qty", "sum"),
            avg_margin=("profit_margin", "mean")
        )
        .reset_index()
        .sort_values("total_sales", ascending=False)
        .head(5)
    )

    # ===== TOP REGIONS, CHANNELS, CUSTOMERS =====
    top_regions = (
        df.groupby("region")["net_extended_line_cost"].sum()
        .reset_index().rename(columns={"net_extended_line_cost": "total_sales"})
        .sort_values("total_sales", ascending=False).head(5)
        if "region" in df.columns else []
    )

    top_channels = (
        df.groupby("channel")["net_extended_line_cost"].sum()
        .reset_index().rename(columns={"net_extended_line_cost": "total_sales"})
        .sort_values("total_sales", ascending=False).head(5)
        if "channel" in df.columns else []
    )

    top_customers = (
        df.groupby("customer_type")["net_extended_line_cost"].sum()
        .reset_index().rename(columns={"net_extended_line_cost": "total_sales"})
        .sort_values("total_sales", ascending=False).head(5)
        if "customer_type" in df.columns else []
    )

    # ===== PAYLOAD =====
    summary_payload = {
        "summary": {
            "start_date": str((start_date or min_date).date()),
            "end_date": str((end_date or max_date).date()),
            "total_orders": int(total_orders),
            "total_customers": int(total_customers),
            "total_revenue": round(total_revenue, 2),
            "average_order_value": round(avg_order_value, 2),
            "total_quantity": int(total_quantity),
            "average_profit_margin": round(avg_profit_margin, 2),
        },
        "top_products": top_products.to_dict(orient="records"),
        "top_regions": (
            top_regions.to_dict(orient="records") if isinstance(top_regions, pd.DataFrame) else []
        ),
        "top_channels": (
            top_channels.to_dict(orient="records") if isinstance(top_channels, pd.DataFrame) else []
        ),
        "top_customers": (
            top_customers.to_dict(orient="records") if isinstance(top_customers, pd.DataFrame) else []
        ),
    }

    # ====== AI BACKGROUND GENERATION ======
    period = "custom"
    cache_key = generate_ai_cache_key(summary_payload, start_date or min_date, end_date or max_date, period)
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, str((start_date or min_date).date()), str((end_date or max_date).date()), period, cache_key, "sales_summary"),
    ).start()

    return Response({
        **summary_payload,
        "data_key": cache_key,
        "ai_status": "processing"
    })

@api_view(["GET"])
def transaction_summary(request):
    """
    Provides a transaction-level summary of sales within the specified date range.
    Uses normalized dataset from load_dataset().
    """
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    period = request.GET.get("period", "custom")
    

    if not start_date or not end_date:
        return Response({"error": "start_date and end_date are required"}, status=400)

    # ===== Validate date inputs =====
    try:
        start_date_parsed = pd.to_datetime(start_date)
        end_date_parsed = pd.to_datetime(end_date)
        if start_date_parsed > end_date_parsed:
            return Response({"error": "start_date cannot be after end_date."}, status=400)
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    # ===== Load dataset =====
    try:
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    # ===== Identify main date column =====
    date_field = "order_date" if "order_date" in df.columns else "main_date"
    if date_field not in df.columns:
        return Response({"error": "No valid date column found in dataset."}, status=500)

    df[date_field] = pd.to_datetime(df[date_field], errors="coerce")
    df = df.dropna(subset=[date_field])

    # ===== Clean numeric fields =====
    for col in ["net_extended_line_cost", "requested_qty"]:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
                .fillna(0)
            )

    # ===== Validate available data range =====
    min_date, max_date = df[date_field].min(), df[date_field].max()
    if (start_date_parsed > max_date) or (end_date_parsed < min_date):
        return Response({
            "error": "Provided date range is outside available dataset.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    # ===== Filter for current period =====
    current_df = df[(df[date_field] >= start_date_parsed) & (df[date_field] <= end_date_parsed)]
    if current_df.empty:
        return Response({"message": "No transactions found for this period."}, status=404)

    # ===== Define previous period =====
    duration = end_date_parsed - start_date_parsed
    prev_start = start_date_parsed - duration - timedelta(days=1)
    prev_end = start_date_parsed - timedelta(days=1)
    previous_df = df[(df[date_field] >= prev_start) & (df[date_field] <= prev_end)]

    # ===== Key metrics =====
    def compute_metrics(subset):
        total_value = subset["net_extended_line_cost"].sum()
        total_quantity = subset["requested_qty"].sum()
        avg_order_value = (
            subset.groupby("order_number")["net_extended_line_cost"].sum().mean()
            if "order_number" in subset.columns else 0
        )
        return total_value, total_quantity, avg_order_value

    cur_val, cur_qty, cur_aov = compute_metrics(current_df)
    prev_val, prev_qty, prev_aov = compute_metrics(previous_df)

    # ===== Helper: percentage change =====
    def pct_change(current, previous):
        if previous == 0:
            return None
        return round(((current - previous) / previous) * 100, 2)

    # ===== Top stores/products =====
    def store_summary(subset):
        if "store_name" not in subset.columns:
            return []
        summary = (
            subset.groupby("store_name")
            .agg(requested_qty=("requested_qty", "sum"),
                 net_sales=("net_extended_line_cost", "sum"))
            .reset_index()
            .sort_values("net_sales", ascending=False)
            .head(20)
        )
        return summary.to_dict(orient="records")

    def product_summary(subset):
        if "product_description" not in subset.columns:
            return []
        summary = (
            subset.groupby("product_description")
            .agg(requested_qty=("requested_qty", "sum"),
                 net_sales=("net_extended_line_cost", "sum"))
            .reset_index()
            .sort_values("net_sales", ascending=False)
            .head(20)
        )
        return summary.to_dict(orient="records")

    # ===== Trend chart =====
    def trend_chart(subset):
        trend = (
            subset.groupby(subset[date_field].dt.date)["net_extended_line_cost"]
            .sum()
            .reset_index()
            .rename(columns={date_field: "date", "net_extended_line_cost": "revenue"})
        )
        trend["revenue"] = trend["revenue"].round(2)
        return trend.to_dict(orient="records")

    # ====== AI background generation ======
    # ✅ Use reusable cache key generator
    cache_key = generate_ai_cache_key(
        "transaction_summary", start_date=start_date, end_date=end_date,period=period,
    )

    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)

    summary_data = {
        "current_total_value": round(cur_val, 2),
        "current_total_quantity": round(cur_qty, 2),
        "current_avg_order_value": round(cur_aov or 0, 2),
        "top_products": product_summary(current_df)[:5],
        "top_stores": store_summary(current_df)[:5],
    }

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_data, start_date, end_date, "custom", cache_key, "transaction_summary"),
    ).start()

    # ===== Response payload =====
    return Response({
        "start_date": str(start_date_parsed.date()),
        "end_date": str(end_date_parsed.date()),
        "ai_cache_key": cache_key,
        "current_period": {
            "total_transaction_value": round(cur_val, 2),
            "total_quantity": round(cur_qty, 2),
            "average_order_value": round(cur_aov or 0, 2),
            "store_summary": store_summary(current_df),
            "product_summary": product_summary(current_df),
            "trend_chart": trend_chart(current_df),
        },
        "previous_period": {
            "total_transaction_value": round(prev_val, 2),
            "total_quantity": round(prev_qty, 2),
            "average_order_value": round(prev_aov or 0, 2),
            "store_summary": store_summary(previous_df),
            "product_summary": product_summary(previous_df),
            "trend_chart": trend_chart(previous_df),
        },
        "percentage_changes": {
            "transaction_value_change": pct_change(cur_val, prev_val),
            "quantity_change": pct_change(cur_qty, prev_qty),
            "average_order_value_change": pct_change(cur_aov, prev_aov),
        },
        "data_key": cache_key,
        "ai_status": "processing",
    })

@api_view(["GET"])
def transaction_entities_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    store_filter = request.GET.get("store")
    sender_filter = request.GET.get("sender")

    # === Load dataset ===
    try:
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    # === Normalize key columns ===
    date_col = "main_date" if "main_date" in df.columns else "order_date"
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["net_extended_line_cost"] = pd.to_numeric(
        df["net_extended_line_cost"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["requested_qty"] = pd.to_numeric(df["requested_qty"], errors="coerce")
    df = df.dropna(subset=[date_col])

    # === Apply date filters ===
    try:
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df[date_col] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df[date_col] <= end]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    # === Apply entity filters ===
    if store_filter:
        df = df[df["store_name"].str.strip().str.lower() == store_filter.strip().lower()]
    if sender_filter:
        df = df[df["sender_name"].str.strip().str.lower() == sender_filter.strip().lower()]

    if df.empty:
        return Response({"message": "No data found for selected filters"}, status=200)

    # === Aggregations ===
    total_revenue = df["net_extended_line_cost"].sum()

    try:
        # Store-level performance
        store_group = (
            df.groupby("store_name")
            .agg(
                revenue=("net_extended_line_cost", "sum"),
                orders=("order_number", "nunique"),
                quantity=("requested_qty", "sum"),
            )
            .round(2)
        )
        store_group["avg_order_value"] = store_group["revenue"] / store_group["orders"]
        store_group["revenue_pct"] = (store_group["revenue"] / total_revenue * 100).round(2)
        top_stores = store_group.sort_values("revenue", ascending=False).head(5).to_dict("index")

        # Sender / Customer-level performance
        customer_group = (
            df.groupby("sender_name")
            .agg(
                revenue=("net_extended_line_cost", "sum"),
                orders=("order_number", "nunique"),
                quantity=("requested_qty", "sum"),
            )
            .round(2)
        )
        customer_group["avg_order_value"] = customer_group["revenue"] / customer_group["orders"]
        customer_group["revenue_pct"] = (customer_group["revenue"] / total_revenue * 100).round(2)
        top_customers_df = customer_group.sort_values("revenue", ascending=False).head(5)
        top_customers = top_customers_df.to_dict("index")

        # Product-level performance
        product_group = (
            df.groupby("product_description")
            .agg(
                revenue=("net_extended_line_cost", "sum"),
                quantity=("requested_qty", "sum"),
                orders=("order_number", "nunique"),
            )
            .round(2)
        )
        product_group["revenue_pct"] = (product_group["revenue"] / total_revenue * 100).round(2)
        top_products = product_group.sort_values("revenue", ascending=False).head(5).to_dict("index")

        # Monthly customer trend
        df["month"] = df[date_col].dt.to_period("M").astype(str)
        top_customer_names = list(top_customers_df.index)
        customer_trend = (
            df[df["sender_name"].isin(top_customer_names)]
            .groupby(["sender_name", "month"])["net_extended_line_cost"]
            .sum()
            .reset_index()
            .pivot(index="month", columns="sender_name", values="net_extended_line_cost")
            .fillna(0)
            .round(2)
        )
        customer_trend_data = customer_trend.reset_index().to_dict(orient="records")

        # === Summary Data for AI Generation ===
        summary_data = {
            "total_revenue": round(total_revenue, 2),
            "top_stores": top_stores,
            "top_customers": top_customers,
            "top_products": top_products,
        }

        # ====== UNIFIED CACHE KEY + AI BACKGROUND GENERATION ======
        cache_key = generate_ai_cache_key(summary_data, start_date, end_date, "transaction_entities")

        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_data, start_date, end_date, "transaction_entities", cache_key, "transaction_entities_analysis"),
        ).start()

    except Exception as e:
        return Response({"error": f"Failed during aggregation: {str(e)}"}, status=500)

    # === Response ===
    return Response({
        "ai_status": "processing",
        "data_key": cache_key,
        "filters_applied": {
            "start_date": start_date,
            "end_date": end_date,
            "store_filter": store_filter,
            "sender_filter": sender_filter,
        },
        "summary": summary_data,
        "top_stores": top_stores,
        "top_customers": top_customers,
        "top_products_by_revenue": top_products,
        "monthly_customer_trend": customer_trend_data,
    })

@api_view(["GET"])
def transaction_timing_analysis(request):
    """
    Analyzes transaction timing and fulfillment patterns from the B2B dataset.
    Frequency breakdown: monthly, weekly, daily, weekday, hourly.
    Also computes fulfillment speed metrics.
    """
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    # === Load dataset ===
    try:
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    # === Validate date fields ===
    if "order_date" not in df.columns:
        return Response({"error": "order_date column missing in dataset."}, status=500)

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")
    df = df.dropna(subset=["order_date"])

    # === Filter by requested range ===
    try:
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df["order_date"] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df["order_date"] <= end]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No transactions found for the selected period"}, status=200)

    try:
        # === Time-based breakdown ===
        df["Month"] = df["order_date"].dt.to_period("M").astype(str)
        df["Week"] = df["order_date"].dt.to_period("W").astype(str)
        df["Day"] = df["order_date"].dt.date.astype(str)
        df["Weekday"] = df["order_date"].dt.day_name()
        df["Hour"] = df["order_date"].dt.hour

        freq_by_month = df.groupby("Month").size().to_dict()
        freq_by_week = df.groupby("Week").size().to_dict()
        freq_by_day = df.groupby("Day").size().to_dict()
        freq_by_weekday = df.groupby("Weekday").size().sort_values(ascending=False).to_dict()
        freq_by_hour = df.groupby("Hour").size().sort_index().to_dict()

        # === Fulfillment metrics ===
        df = df.dropna(subset=["delivery_date"])
        df["Fulfillment Days"] = (df["delivery_date"] - df["order_date"]).dt.days

        avg_fulfillment = df["Fulfillment Days"].mean()
        best_fulfillment = df["Fulfillment Days"].min()
        worst_fulfillment = df["Fulfillment Days"].max()

        # === Fulfillment trend ===
        df["Month"] = df["order_date"].dt.to_period("M").astype(str)
        fulfillment_trend = df.groupby("Month")["Fulfillment Days"].mean().round(2).to_dict()

    except Exception as e:
        return Response({"error": f"Error during aggregation: {str(e)}"}, status=500)

    # === Background AI analysis ===
    cache_key = f"transaction_timing_analysis:{start_date or 'null'}:{end_date or 'null'}"
    period = "month"

    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    summary = {
        "frequency_by_weekday": freq_by_weekday,
        "fulfillment_summary": {
            "average_days": round(avg_fulfillment, 2) if not pd.isna(avg_fulfillment) else None,
            "fastest_days": int(best_fulfillment) if not pd.isna(best_fulfillment) else None,
            "slowest_days": int(worst_fulfillment) if not pd.isna(worst_fulfillment) else None,
        }
    }

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary, start_date, end_date, period, cache_key, "transaction_timing_analysis"),
    ).start()

    # === Final response ===
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
        },
        "data_key": cache_key
    })

@api_view(["GET"])
def product_demand_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    # === Load Dataset ===
    try:
        df = load_dataset()  # uses normalized columns like "product_description", "requested_qty", "main_date"
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    # === Clean and Prepare Data ===
    df["requested_qty"] = pd.to_numeric(df["requested_qty"], errors="coerce")
    df["net_extended_line_cost"] = pd.to_numeric(
        df["net_extended_line_cost"].astype(str).str.replace(",", ""),
        errors="coerce",
    )
    df["unit_selling_price"] = pd.to_numeric(df["unit_selling_price"], errors="coerce")
    df["unit_cost_price"] = pd.to_numeric(df["unit_cost_price"], errors="coerce")

    df = df.dropna(subset=["main_date", "requested_qty"])
    df = df[df["requested_qty"] > 0]

    # === Apply Date Filters ===
    try:
        if start_date:
            df = df[df["main_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["main_date"] <= pd.to_datetime(end_date)]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No data found in the selected date range."}, status=200)

    # === Enrichment Columns ===
    df["month"] = df["main_date"].dt.to_period("M").astype(str)
    df["weekday"] = df["main_date"].dt.day_name()
    df["profit_per_unit"] = df["unit_selling_price"] - df["unit_cost_price"]
    df["total_profit"] = df["profit_per_unit"] * df["requested_qty"]

    try:
        # === Top Products by Quantity ===
        top_products = (
            df.groupby("product_description")["requested_qty"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        # === Demand Trend (Over Time) ===
        trend = (
            df.groupby(df["main_date"].dt.date)["requested_qty"]
            .sum()
            .reset_index()
            .rename(columns={"main_date": "date", "requested_qty": "quantity"})
        )

        # === Store-level Demand ===
        store_demand = (
            df.groupby(["store_name", "product_description"])["requested_qty"]
            .sum()
            .reset_index()
            .sort_values(by="requested_qty", ascending=False)
        )

        # === Velocity (Average Quantity per Order) ===
        velocity = (
            df.groupby(["product_description", "order_number"])["requested_qty"]
            .sum()
            .groupby("product_description")
            .mean()
            .sort_values(ascending=False)
            .head(10)
        )

        # === Profitability by Product ===
        product_profit = (
            df.groupby("product_description")["total_profit"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        # === Monthly & Weekday Seasonality ===
        by_month = df.groupby("month")["requested_qty"].sum().to_dict()
        by_weekday = (
            df.groupby("weekday")["requested_qty"]
            .sum()
            .sort_values(ascending=False)
            .to_dict()
        )

        # === Rising Demand Detection ===
        parsed_start = pd.to_datetime(start_date) if start_date else df["main_date"].min()
        parsed_end = pd.to_datetime(end_date) if end_date else df["main_date"].max()
        period_length = parsed_end - parsed_start

        prev_start = parsed_start - period_length
        prev_end = parsed_start - timedelta(days=1)

        prev_df = df[(df["main_date"] >= prev_start) & (df["main_date"] <= prev_end)]

        recent_top = df.groupby("product_description")["requested_qty"].sum()
        prev_top = prev_df.groupby("product_description")["requested_qty"].sum()

        combined_demand = pd.concat(
            [recent_top, prev_top], axis=1, keys=["current", "previous"]
        ).fillna(0)
        combined_demand["pct_change"] = combined_demand.apply(safe_pct_change, axis=1)
        rising_demand = (
            combined_demand.sort_values("pct_change", ascending=False)
            .head(5)
            .round(2)
            .to_dict(orient="index")
        )

        # === Product Demand Matrix (Store vs Product) ===
        matrix = df.pivot_table(
            index="store_name",
            columns="product_description",
            values="requested_qty",
            aggfunc="sum",
            fill_value=0,
        )

        # === Supplier Performance (Optional) ===
        top_suppliers = (
            df.groupby("supplier_name")["requested_qty"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        )

    except Exception as e:
        return Response({"error": f"Failed to compute demand analysis: {str(e)}"}, status=500)

    # === AI Background Processing ===
    period = "month"
    summary = {
        "top_products": top_products.head(5).round(2).to_dict(),
        "rising_demand": rising_demand,
        "velocity": velocity.head(5).round(2).to_dict(),
        "product_profitability": product_profit.head(5).round(2).to_dict(),
        "monthly_demand": by_month,
        "supplier_performance": top_suppliers,
    }

    # ✅ Use standardized AI cache key
    cache_key = generate_ai_cache_key(summary, start_date, end_date, period)

    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary, start_date, end_date, period, cache_key, "product_demand_analysis"),
    ).start()

    # === Response ===
    return Response({
        "summary": {
            "top_products_by_quantity": top_products.round(2).to_dict(),
            "top_profitable_products": product_profit.round(2).to_dict(),
            "top_suppliers": top_suppliers,
        },
        "demand_trend_over_time": trend.to_dict(orient="records"),
        "store_product_demand": store_demand.to_dict(orient="records"),
        "demand_velocity_per_product": velocity.round(2).to_dict(),
        "seasonality": {
            "monthly_demand": {k: round(v, 2) for k, v in by_month.items()},
            "weekday_demand": {k: round(v, 2) for k, v in by_weekday.items()},
        },
        "rising_product_demand": rising_demand,
        "product_demand_matrix": matrix.astype(int).to_dict(),
        "data_key": cache_key,
        "status": "processing"
    })

@api_view(["GET"])
def product_revenue_analysis(request):
    """
    Analyze revenue performance across products, stores, and time.
    Includes AI-driven insights and forecasting via background task.
    """
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        # === Load and prepare dataset ===
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    try:
        # === Ensure correct data types ===
        df["net_extended_line_cost"] = pd.to_numeric(
            df["net_extended_line_cost"].astype(str).str.replace(",", ""), errors="coerce"
        )
        df = df.dropna(subset=["main_date", "net_extended_line_cost"])
    except Exception as e:
        return Response({"error": f"Failed to normalize data: {str(e)}"}, status=500)

    # === Apply date filtering ===
    try:
        if start_date:
            start = pd.to_datetime(start_date)
            df = df[df["main_date"] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            df = df[df["main_date"] <= end]
    except Exception as e:
        return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No data found in the selected period."}, status=200)

    try:
        # === Derived fields ===
        df["weekday"] = df["main_date"].dt.day_name()
        df["month"] = df["main_date"].dt.to_period("M").astype(str)

        # === Top Products by Revenue ===
        top_products = (
            df.groupby("product_description")["net_extended_line_cost"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        # === Revenue Trend (daily) ===
        trend = (
            df.groupby(df["main_date"].dt.date)["net_extended_line_cost"]
            .sum()
            .reset_index()
            .rename(columns={"main_date": "date", "net_extended_line_cost": "revenue"})
        )

        # === Store-Product Revenue Matrix ===
        store_revenue = (
            df.groupby(["store_name", "product_description"])["net_extended_line_cost"]
            .sum()
            .reset_index()
            .sort_values(by="net_extended_line_cost", ascending=False)
        )

        # === Average Revenue per Product per Order (Revenue Yield) ===
        if "order_number" in df.columns:
            order_revenue = (
                df.groupby(["product_description", "order_number"])["net_extended_line_cost"]
                .sum()
                .reset_index()
            )
            revenue_yield = (
                order_revenue.groupby("product_description")["net_extended_line_cost"]
                .mean()
                .sort_values(ascending=False)
                .head(10)
            )
        else:
            revenue_yield = (
                df.groupby("product_description")["net_extended_line_cost"]
                .mean()
                .sort_values(ascending=False)
                .head(10)
            )

        # === Seasonality ===
        by_month = df.groupby("month")["net_extended_line_cost"].sum().to_dict()
        by_weekday = (
            df.groupby("weekday")["net_extended_line_cost"]
            .sum()
            .sort_values(ascending=False)
            .to_dict()
        )

        # === Rising Product Revenue (Period-on-Period Comparison) ===
        parsed_start = pd.to_datetime(start_date) if start_date else df["main_date"].min()
        parsed_end = pd.to_datetime(end_date) if end_date else df["main_date"].max()
        period_length = parsed_end - parsed_start

        prev_start = parsed_start - period_length
        prev_end = parsed_start - timedelta(days=1)
        prev_df = df[(df["main_date"] >= prev_start) & (df["main_date"] <= prev_end)]

        recent_revenue = df.groupby("product_description")["net_extended_line_cost"].sum()
        prev_revenue = prev_df.groupby("product_description")["net_extended_line_cost"].sum()

        combined_revenue = pd.concat(
            [recent_revenue, prev_revenue],
            axis=1,
            keys=["current", "previous"],
        ).fillna(0)

        combined_revenue["pct_change"] = combined_revenue.apply(safe_pct_change, axis=1)

        rising_revenue = (
            combined_revenue.sort_values(
                by="pct_change",
                ascending=False,
                key=lambda x: x.map(
                    lambda v: float("-inf") if v == 0 else (float("inf") if v == "new product" else v)
                ),
            )
            .head(5)
            .round(2)
            .to_dict(orient="index")
        )

        # === Pivot Matrix (Store vs Product) ===
        matrix = df.pivot_table(
            index="store_name",
            columns="product_description",
            values="net_extended_line_cost",
            aggfunc="sum",
            fill_value=0,
        )

    except Exception as e:
        return Response({"error": f"Failed to compute revenue analysis: {str(e)}"}, status=500)

    # === AI Analytics Integration ===
    period = "month"

    summary_payload = {
        "top_products": top_products.head(5).round(2).to_dict(),
        "rising_revenue": rising_revenue,
        "revenue_yield": revenue_yield.head(5).round(2).to_dict(),
        "monthly_revenue": by_month,
    }

    # ✅ Generate AI cache key dynamically
    cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, period)

    # Initialize AI cache placeholders
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    # Background AI analysis
    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, start_date, end_date, period, cache_key, "product_revenue_analysis"),
    ).start()

    # === Final Response ===
    return Response({
        "top_products_by_revenue": top_products.round(2).to_dict(),
        "revenue_trend_over_time": trend.round(2).to_dict(orient="records"),
        "store_product_revenue": store_revenue.round(2).to_dict(orient="records"),
        "revenue_yield_per_product": revenue_yield.round(2).to_dict(),
        "seasonality": {
            "monthly_revenue": {k: round(v, 2) for k, v in by_month.items()},
            "weekday_revenue": {k: round(v, 2) for k, v in by_weekday.items()},
        },
        "rising_product_revenue": rising_revenue,
        "product_revenue_matrix": matrix.round(2).to_dict(),
        "data_key": cache_key,
    })

@api_view(["GET"])
def product_correlation_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    store_filter = request.GET.get("store")
    sender_filter = request.GET.get("sender")

    try:
        combined_df, sales_df, invoice_df = load_data()
        df = combined_df
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    # === FILTERING ===
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
        # === BUILD PRODUCT BASKET ===
        basket = df.groupby(["Order Number", "Product Description"])["Requested Qty"].sum().unstack(fill_value=0)
        if basket.empty or basket.shape[1] < 2:
            return Response({"message": "Not enough overlapping product orders to compute correlation."}, status=200)

        binary_basket = (basket > 0).astype(int)

        # === BASIC CO-OCCURRENCE ===
        order_groups = df.groupby("Order Number")["Product Description"].apply(set)
        pairs = []
        for products in order_groups:
            if len(products) > 1:
                pairs.extend(combinations(products, 2))

        if not pairs:
            return Response({"message": "Not enough co-occurring product pairs for analysis."}, status=200)

        pair_counts = Counter(pairs)
        total_orders = len(order_groups)

        # === METRIC CALCULATIONS ===
        metrics = []
        for (prod_a, prod_b), count in pair_counts.items():
            a_orders = binary_basket[prod_a].sum()
            b_orders = binary_basket[prod_b].sum()

            support_a = a_orders / total_orders
            support_b = b_orders / total_orders
            support_ab = count / total_orders

            # Association Rule Metrics
            confidence = round(support_ab / support_a, 3) if support_a else 0
            lift = round(confidence / support_b, 3) if support_b else 0

            # Jaccard Similarity
            jaccard = round(support_ab / (support_a + support_b - support_ab), 3) if (support_a + support_b - support_ab) else 0

            # Cosine Similarity
            vec_a, vec_b = binary_basket[prod_a], binary_basket[prod_b]
            cosine_sim = round(1 - cosine(vec_a, vec_b), 3)

            # Pearson Correlation (Demand Relationship)
            pearson_corr, _ = pearsonr(basket[prod_a], basket[prod_b])
            pearson_corr = round(pearson_corr, 3) if not np.isnan(pearson_corr) else 0

            # Affinity (your original metric)
            denominator = a_orders + b_orders - count
            affinity = round(count / denominator, 3) if denominator else 0

            metrics.append({
                "product_pair": f"{prod_a} & {prod_b}",
                "co_occurrence": int(count),
                "support": round(support_ab, 3),
                "confidence": confidence,
                "lift": lift,
                "jaccard": jaccard,
                "cosine_similarity": cosine_sim,
                "pearson_correlation": pearson_corr,
                "affinity_score": affinity
            })

        metrics_df = pd.DataFrame(metrics)
        top_metrics = metrics_df.sort_values("lift", ascending=False).head(10).to_dict(orient="records")

        # === CENTRALITY ===
        product_links = Counter()
        for (a, b), count in pair_counts.items():
            product_links[a] += count
            product_links[b] += count
        central_products = dict(product_links.most_common(10))

    except Exception as e:
        return Response({"error": f"Failed to process correlation analysis: {str(e)}"}, status=500)

    

    summary_payload = {
        "top_correlations": top_metrics,
        "central_products": central_products
    }
    # === AI INSIGHT THREAD ===
    cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, period="month")
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)
    
    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, start_date, end_date, "month", cache_key, "product_correlation_analysis")
    ).start()

    return Response({
        "top_product_relationships": top_metrics,
        "top_central_products": central_products,
        "data_key": cache_key,
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
        df = load_dataset()  # Load current full dataset
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    # === OPTIONAL FILTERS ===
    try:
        if start_date:
            df = df[df["main_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["main_date"] <= pd.to_datetime(end_date)]
        if store_filter:
            df = df[df["store_name"].str.lower() == store_filter.lower()]
        if sender_filter:
            df = df[df["sender_name"].str.lower() == sender_filter.lower()]
        if product_filter:
            df = df[df["product_description"].str.lower().str.contains(product_filter.lower())]
    except Exception as e:
        return Response({"error": f"Error during filtering: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No data found for selected filters."}, status=200)

    try:
        # === CLEAN AND PREPARE DATA ===
        df["requested_qty"] = pd.to_numeric(df["requested_qty"], errors="coerce").fillna(0)
        df["net_extended_line_cost"] = pd.to_numeric(df["net_extended_line_cost"], errors="coerce").fillna(0)
        df["order_number"] = df["order_number"].astype(str)

        df["period"] = df["main_date"].dt.to_period(granularity).astype(str)

        # === TREND CALCULATIONS ===
        revenue_trend = df.groupby(["period", "product_description"])["net_extended_line_cost"].sum().reset_index()
        quantity_trend = df.groupby(["period", "product_description"])["requested_qty"].sum().reset_index()
        freq_trend = df.groupby(["period", "product_description"])["order_number"].nunique().reset_index()

        revenue_pivot = revenue_trend.pivot(index="period", columns="product_description", values="net_extended_line_cost").fillna(0)
        quantity_pivot = quantity_trend.pivot(index="period", columns="product_description", values="requested_qty").fillna(0)
        freq_pivot = freq_trend.pivot(index="period", columns="product_description", values="order_number").fillna(0)

        product_totals = df.groupby("product_description")["net_extended_line_cost"].sum().sort_values(ascending=False)
        top_products = product_totals.head(top_n).index.tolist()
        if not top_products:
            return Response({"message": "No product trends available for top products."}, status=200)

        # Trend summary for last 3 periods
        recent_periods = sorted(df["period"].unique())[-3:]
        trend_summary = {}
        for product in top_products:
            try:
                product_series = revenue_pivot[product].reindex(recent_periods, fill_value=0)
                direction = (
                    "increasing" if product_series.iloc[-1] > product_series.iloc[0] else
                    "declining" if product_series.iloc[-1] < product_series.iloc[0] else
                    "flat"
                )
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

    # === BACKGROUND AI THREAD USING generate_ai_cache_key ===
    summary_payload = {
        "top_products": list(product_totals.head(top_n).round(2).to_dict().items()),
        "trend_summary": trend_summary
    }
    cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, granularity)

    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, start_date, end_date, granularity, cache_key, "product_trend_analysis")
    ).start()

    return Response({
        "revenue_trend": revenue_pivot[top_products].round(2).to_dict(orient="index"),
        "quantity_trend": quantity_pivot[top_products].round(2).to_dict(orient="index"),
        "frequency_trend": freq_pivot[top_products].astype(int).to_dict(orient="index"),
        "top_products": product_totals.head(top_n).round(2).to_dict(),
        "trend_summary": trend_summary,
        "data_key": cache_key
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
        df = load_dataset()  # Load current full dataset

        # Clean numeric columns
        df["unit_cost_price"] = pd.to_numeric(df.get("unit_cost_price", 0), errors="coerce").fillna(0)
        df["unit_selling_price"] = pd.to_numeric(df.get("unit_selling_price", 0), errors="coerce").fillna(0)
        df["net_extended_line_cost"] = pd.to_numeric(df.get("net_extended_line_cost", 0), errors="coerce").fillna(0)
        df["requested_qty"] = pd.to_numeric(df.get("requested_qty", 0), errors="coerce").fillna(0)

    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    try:
        # Optional filters
        if start_date:
            df = df[df["main_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["main_date"] <= pd.to_datetime(end_date)]
        if store_filter:
            df = df[df["store_name"].str.lower() == store_filter.lower()]
        if sender_filter:
            df = df[df["sender_name"].str.lower() == sender_filter.lower()]
        if product_filter:
            df = df[df["product_description"].str.lower().str.contains(product_filter.lower())]

        if df.empty:
            return Response({"message": "No data available for the selected filters."}, status=200)

        # Use main_date as created_date
        df["created_date"] = df["main_date"]

        # Basic metrics
        total_orders = df["order_number"].nunique()
        total_value = df["net_extended_line_cost"].sum()
        unique_products = df["product_description"].nunique()
        avg_order_value = df.groupby("order_number")["net_extended_line_cost"].sum(min_count=1).mean()
        items_per_order = df.groupby("order_number")["requested_qty"].sum(min_count=1).mean()

        # Order trend
        df["period"] = df["created_date"].dt.to_period(granularity).astype(str)
        order_trend_df = df.groupby("period")["order_number"].nunique().reset_index()
        order_trend = order_trend_df.set_index("period")["order_number"].to_dict()

        # Compute percentage change between periods
        order_trend_df["pct_change"] = order_trend_df["order_number"].pct_change().fillna(0) * 100
        trend_directions = {}
        for idx, row in order_trend_df.iterrows():
            if row["pct_change"] > 0:
                trend_directions[row["period"]] = "increasing"
            elif row["pct_change"] < 0:
                trend_directions[row["period"]] = "declining"
            else:
                trend_directions[row["period"]] = "flat"

        # Top customers and stores
        top_customers = df.groupby("sender_name")["order_number"].nunique().sort_values(ascending=False).head(5).to_dict()
        top_stores = df.groupby("store_name")["order_number"].nunique().sort_values(ascending=False).head(5).to_dict()

        # Fulfillment metrics
        df["delivery_date"] = pd.to_datetime(df.get("delivery_date"), errors="coerce")
        df_fulfilled = df.dropna(subset=["delivery_date"])
        df_fulfilled["fulfillment_days"] = (df_fulfilled["delivery_date"] - df_fulfilled["created_date"]).dt.days

        fulfillment_stats = {
            "average_days": round(df_fulfilled["fulfillment_days"].mean(), 2) if not df_fulfilled.empty else None,
            "max_days": int(df_fulfilled["fulfillment_days"].max()) if not df_fulfilled["fulfillment_days"].empty else None,
            "min_days": int(df_fulfilled["fulfillment_days"].min()) if not df_fulfilled["fulfillment_days"].empty else None,
        }

    except Exception as e:
        return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

    # --- AI Background Thread ---
    summary_payload = {
        "total_orders": total_orders,
        "unique_products": unique_products,
        "total_order_value": round(total_value, 2),
        "average_order_value": round(avg_order_value or 0, 2),
        "average_items_per_order": round(items_per_order or 0, 2),
        "order_volume_trend": order_trend,
        "order_volume_trend_directions": trend_directions,
        "top_customers": top_customers,
        "top_stores": top_stores,
        "fulfillment_stats": fulfillment_stats,
    }
    cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, granularity)

    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, start_date, end_date, granularity, cache_key, "order_analysis")
    ).start()

    return Response({
        "total_orders": total_orders,
        "unique_products_ordered": unique_products,
        "total_order_value": round(total_value, 2),
        "average_order_value": round(avg_order_value or 0, 2),
        "average_items_per_order": round(items_per_order or 0, 2),
        "percentage_change_between_periods": order_trend,
        "order_volume_trend_directions": trend_directions,
        "top_customers_by_orders": top_customers,
        "top_stores_by_orders": top_stores,
        "fulfillment_stats": fulfillment_stats,
        "data_key": cache_key
    })

@api_view(["GET"])
def order_fulfillment_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    sla_param = request.GET.get("sla", 1)

    try:
        sla_days = int(sla_param)
    except (TypeError, ValueError):
        return Response({"error": "Invalid SLA value. It must be an integer."}, status=400)

    try:
        df = load_dataset()  # Load the current dataset

        # Normalize numeric/date columns
        df["main_date"] = pd.to_datetime(df.get("main_date", pd.NaT), errors="coerce")
        df["delivery_date"] = pd.to_datetime(df.get("delivery_date", pd.NaT), errors="coerce")
        df["order_number"] = df.get("order_number", "").astype(str)

    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    try:
        # Optional date filtering
        if start_date:
            df = df[df["main_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["main_date"] <= pd.to_datetime(end_date)]

        if df.empty:
            return Response({"message": "No data found in the selected period."}, status=200)

        total_orders = df["order_number"].nunique()

        # Canceled orders
        canceled_orders_df = df[df["delivery_date"].isna()]
        canceled_orders = canceled_orders_df["order_number"].nunique()
        cancellation_rate = round((canceled_orders / total_orders) * 100, 2) if total_orders else 0

        # Fulfilled orders
        fulfilled_df = df.dropna(subset=["delivery_date"]).copy()
        fulfilled_df["fulfillment_days"] = (fulfilled_df["delivery_date"] - fulfilled_df["main_date"]).dt.days

        if fulfilled_df.empty:
            return Response({"message": "No fulfilled orders in this period."}, status=200)

        # Fulfillment stats
        stats = fulfilled_df["fulfillment_days"].describe().round(2).to_dict()
        stats["std"] = round(fulfilled_df["fulfillment_days"].std(), 2)

        # SLA compliance
        within_sla = (fulfilled_df["fulfillment_days"] <= sla_days).sum()
        total_fulfilled_orders = fulfilled_df["order_number"].nunique()
        sla_pct = round((within_sla / total_fulfilled_orders) * 100, 2) if total_fulfilled_orders else 0

        # Delivery efficiency
        delivery_rate = round((total_fulfilled_orders / total_orders) * 100, 2) if total_orders else 0
        delivery_efficiency_score = round((delivery_rate * sla_pct) / 100, 2)

        # Performance by store and sender
        by_store = fulfilled_df.groupby("store_name")["fulfillment_days"].mean().round(2).sort_values().to_dict()
        by_sender = fulfilled_df.groupby("sender_name")["fulfillment_days"].mean().round(2).sort_values().to_dict()

        # Top delays
        delayed = fulfilled_df[fulfilled_df["fulfillment_days"] > sla_days]
        top_delays = delayed.sort_values("fulfillment_days", ascending=False)
        top_delays = top_delays[["order_number", "store_name", "sender_name", "fulfillment_days"]].head(5).to_dict(orient="records")

        # Distribution
        dist = fulfilled_df["fulfillment_days"].value_counts().sort_index().to_dict()

        # --- AI Background Processing ---
        summary_payload = {
            "fulfillment_statistics": stats,
            "percent_within_sla": sla_pct,
            "delivery_rate": delivery_rate,
            "delivery_efficiency_score": delivery_efficiency_score,
            "cancellation_rate": cancellation_rate,
            "fulfillment_distribution": dist,
            "top_delayed_orders": top_delays,
            "average_fulfillment_by_store": by_store,
            "average_fulfillment_by_sender": by_sender
        }

        cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, sla_days)
        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_payload, start_date, end_date, sla_days, cache_key, "order_fulfillment_analysis")
        ).start()

        return Response({
            "fulfillment_statistics": stats,
            "percent_within_sla": sla_pct,
            "delivery_rate": delivery_rate,
            "delivery_efficiency_score": delivery_efficiency_score,
            "cancellation_rate": cancellation_rate,
            "fulfillment_distribution": dist,
            "top_delayed_orders": top_delays,
            "average_fulfillment_by_store": by_store,
            "average_fulfillment_by_sender": by_sender,
            "data_key": cache_key,
            "status": "processing"
        })

    except Exception as e:
        return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

'''DEPRECATED ORDER CALCULATION'''
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
        combined_df, sales_df, invoice_df = load_data()
        df= combined_df
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    try:        
        df = filter_by_date(df, start_date, end_date)
        if df.empty:
            return Response({"message": "No order data found in this period."}, status=200)

        df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        df = df.dropna(subset=["Created Date"])

        # 🔧 CLEAN NUMERIC FIELDS
        for col in ["Net Extended Line Cost", "Requested Qty", "Cost Price"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"[^0-9.\-]", "", regex=True)
                    .apply(pd.to_numeric, errors="coerce")
                )


        # Aggregate order-level metrics
        order_value = df.groupby("Order Number")["Net Extended Line Cost"].sum()
        order_items = df.groupby("Order Number")["Requested Qty"].sum()
        order_products = df.groupby("Order Number")["Product Description"].nunique()

        total_orders = order_value.count()
        total_lines = df.shape[0]
        avg_order_value = round(order_value.mean(), 2) if not order_value.empty else 0
        avg_items = round(order_items.mean(), 2) if not order_items.empty else 0
        avg_products = round(order_products.mean(), 2) if not order_products.empty else 0

        # High/low value orders
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

        # --- AI Background Processing ---
        cache_key = f"order_calculation_analysis:{start_date or 'null'}:{end_date or 'null'}:{threshold_param}"
        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=({
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
            }, start_date, end_date, threshold_param, cache_key, "order_calculation_analysis")
        ).start()

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
            "order_frequency_trend": order_freq,
            "data_key": cache_key
        })

    except Exception as e:
        return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

# @api_view(["GET"])
# def customer_segmentation_analysis(request):
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
        return Response({"message": "No transaction data found for the selected period."}, status=200)

    try:
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
        churn_threshold = 30
        churned_customers = rfm[rfm["Recency"] > churn_threshold].sort_values("Recency", ascending=False)
        churn_list = churned_customers[["Customer", "Recency", "Monetary"]].head(10).round(2).to_dict(orient="records")

        # === AI Background Task Trigger ===
        cache_key = f"customer_segmentation_analysis:{start_date or 'null'}:{end_date or 'null'}"
        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=({
                "rfm": rfm.round(2).to_dict(orient="records"),
                "summary": {
                    "total_customers": rfm.shape[0],
                    "high_value_customers": int((rfm["Segment"] == "High").sum()),
                    "low_value_customers": int((rfm["Segment"] == "Low").sum()),
                },
                "revenue_over_time": revenue_over_time,
                "top_growing_customers": top_growing_customers,
                "churned_customers": churn_list
            }, start_date, end_date, None, cache_key, "customer_segmentation_analysis")
        ).start()

        return Response({
            "customer_rfm": rfm.round(2).to_dict(orient="records"),
            "summary": {
                "total_customers": rfm.shape[0],
                "high_value_customers": int((rfm["Segment"] == "High").sum()),
                "low_value_customers": int((rfm["Segment"] == "Low").sum()),
            },
            "revenue_over_time": revenue_over_time,
            "top_growing_customers": top_growing_customers,
            "churned_customers": churn_list,
            "data_key": cache_key
        })
    except Exception as e:
        return Response({"error": f"Failed to compute customer segmentation: {str(e)}"}, status=500)

@api_view(["GET"])
def customer_segmentation_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    today = pd.Timestamp.today()

    try:
        # ===== LOAD DATA =====
        df = load_dataset()

        # Ensure numeric fields
        numeric_cols = ["requested_qty", "unit_cost_price", "unit_selling_price", "net_extended_line_cost"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Ensure datetime fields
        if "order_date" not in df.columns:
            return Response({"error": "Dataset missing required column: 'order_date'"}, status=500)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce", dayfirst=True)
        df.dropna(subset=["order_date"], inplace=True)

        if "delivery_date" in df.columns:
            df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce", dayfirst=True)

        # Optional filters
        if start_date:
            df = df[df["order_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["order_date"] <= pd.to_datetime(end_date)]

        if df.empty:
            return Response({"message": "No transaction data found for the selected period."}, status=200)

        # ===== RFM / CLV / CHURN =====
        rfm = compute_rfm(df, today, date_col="order_date")
        clv = compute_clv(df)
        churn_df, churn_rate = compute_churn(df, today, date_col="order_date")


        # Merge RFM + CLV + Churn using normalized sender_name
        rfm = rfm.merge(clv[["sender_name", "CLV"]], left_on="Customer", right_on="sender_name", how="left").drop("sender_name", axis=1)
        rfm = rfm.merge(churn_df[["sender_name", "Status"]], left_on="Customer", right_on="sender_name", how="left") \
                 .drop("sender_name", axis=1).rename(columns={"Status": "Customer Status"})

        # ===== REVENUE OVER TIME =====
        df["period"] = df["order_date"].dt.to_period("M").astype(str)
        revenue_time = df.groupby(["period", "sender_name"])["net_extended_line_cost"].sum().reset_index()
        revenue_pivot = revenue_time.pivot(index="period", columns="sender_name", values="net_extended_line_cost").fillna(0).round(2)
        revenue_over_time = revenue_pivot.to_dict(orient="index")

        # ===== TOP GROWING CUSTOMERS =====
        if len(revenue_pivot.index) >= 2:
            latest_two = revenue_pivot.iloc[-2:]
            revenue_diff = latest_two.diff().iloc[-1].sort_values(ascending=False)
            top_growing_customers = revenue_diff.head(5).round(2).to_dict()
        else:
            top_growing_customers = {}

        churn_list = rfm[rfm["Customer Status"] == "Churned"][["Customer", "Recency", "Monetary", "CLV"]] \
                        .head(10).round(2).to_dict(orient="records")

        # ===== CACHE + THREAD =====
        summary_payload = {
            "rfm": rfm.round(2).to_dict(orient="records"),
            "summary": {
                "total_customers": rfm.shape[0],
                "high_value_customers": int((rfm["Segment"] == "High Value").sum()),
                "low_value_customers": int((rfm["Segment"] == "Low Value").sum()),
                "churn_rate": churn_rate,
                "active_customers": int((rfm["Customer Status"] == "Active").sum()),
                "churned_customers": int((rfm["Customer Status"] == "Churned").sum())
            },
            "revenue_over_time": revenue_over_time,
            "top_growing_customers": top_growing_customers,
            "churned_customers": churn_list
        }
        cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, period="monthly")

        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_payload, start_date, end_date, None, cache_key, "customer_segmentation_analysis")
        ).start()

        return Response({
            "customer_rfm": rfm.round(2).to_dict(orient="records"),
            "summary": summary_payload["summary"],
            "revenue_over_time": revenue_over_time,
            "top_growing_customers": top_growing_customers,
            "churned_customers": churn_list,
            "data_key": cache_key
        })

    except Exception as e:
        return Response({"error": f"Failed to compute segmentation: {str(e)}"}, status=500)


@api_view(["GET"])
def customer_purchase_pattern_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_dataset()

        # Ensure numeric fields
        numeric_cols = ["requested_qty", "unit_cost_price", "unit_selling_price", "net_extended_line_cost"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Ensure datetime
        if "order_date" not in df.columns:
            return Response({"error": "Dataset missing required column: 'order_date'"}, status=500)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce", dayfirst=True)
        df.dropna(subset=["order_date"], inplace=True)

        if start_date:
            df = df[df["order_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["order_date"] <= pd.to_datetime(end_date)]

        if df.empty:
            return Response({"message": "No transaction data for selected period."}, status=200)

        # ===== COMPUTE PURCHASE PATTERNS =====
        patterns, orders_by_weekday, orders_by_month = compute_purchase_patterns(df, date_col="order_date")

        # ===== GENERATE CACHE KEY =====
        summary_payload = {
            "purchase_patterns": patterns.to_dict(orient="records"),
            "orders_by_weekday": orders_by_weekday.to_dict(orient="records"),
            "orders_by_month": orders_by_month.to_dict(orient="records")
        }
        cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, "monthly")

        # ===== CACHE PLACEHOLDERS =====
        cache.set(cache_key + ":status", {"insight": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)

        # ===== START BACKGROUND THREAD =====
        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_payload, start_date, end_date, None, cache_key, "customer_purchase_patterns")
        ).start()

        return Response({
            "purchase_patterns": summary_payload["purchase_patterns"],
            "orders_by_weekday": summary_payload["orders_by_weekday"],
            "orders_by_month": summary_payload["orders_by_month"],
            "data_key": cache_key
        })

    except Exception as e:
        return Response({"error": f"Failed to compute purchase patterns: {str(e)}"}, status=500)

@api_view(["GET"])
def retail_inventory_analysis(request):
    try:
        start_date = request.GET.get("start_date")
        end_date = request.GET.get("end_date")
        store_filter = request.GET.get("store")
        supplier_filter = request.GET.get("supplier")

        today = pd.Timestamp.today()

        # Load dataset
        df = load_dataset()

        # Numeric fields
        numeric_cols = [
            "inventory_level_before_sale",
            "warehouse_utilization",
            "logistics_cost_per_unit",
            "operational_cost_total",
            "forecasted_demand_next_period",
            "actual_demand",
            "lead_time_days"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Date filtering
        if start_date:
            df = df[df["order_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["order_date"] <= pd.to_datetime(end_date)]

        # Store/Supplier filtering
        if store_filter:
            df = df[df["store_name"] == store_filter]
        if supplier_filter:
            df = df[df["supplier_name"] == supplier_filter]

        if df.empty:
            return Response({"message": "No inventory data found for the selected filters."}, status=200)

        # Inventory aggregation
        inventory_summary = df.groupby("store_name").agg({
            "inventory_level_before_sale": "sum",
            "stock_replenishment_date": "count",
            "warehouse_utilization": "mean",
            "logistics_cost_per_unit": "mean",
            "operational_cost_total": "sum",
            "forecasted_demand_next_period": "sum",
            "actual_demand": "sum",
            "lead_time_days": "mean"
        }).reset_index()

        inventory_summary = inventory_summary.round({
            "warehouse_utilization": 1,
            "logistics_cost_per_unit": 2,
            "lead_time_days": 1})

        # ================= FORECAST ACCURACY =================
        # Forecast accuracy
        if "forecasted_demand_next_period" in df.columns and "actual_demand" in df.columns:
            df["forecast_accuracy_%"] = 100 * (
                1 - abs(df["forecasted_demand_next_period"] - df["actual_demand"]) / df["actual_demand"].replace(0,1)
            )
            forecast_accuracy_summary = df.groupby("store_name")["forecast_accuracy_%"].mean().round(1).to_dict()
        else:
            forecast_accuracy_summary = {}

        # ================= AI INSIGHT CACHING =================
        summary_payload = {
            "inventory_summary": inventory_summary.to_dict(orient="records"),
            "forecast_accuracy": forecast_accuracy_summary
        }
        cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, "monthly")

        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_payload, start_date, end_date, None, cache_key, "retail_inventory_analysis")
        ).start()

        return Response({
            "inventory_summary": summary_payload["inventory_summary"],
            # "forecast_accuracy": forecast_accuracy_summary,
            "data_key": cache_key
        })

    except Exception as e:
        return Response({"error": f"Failed to compute inventory analytics: {str(e)}"}, status=500)


@api_view(["GET"])
def promotion_analysis(request):
    try:
        start_date = request.GET.get("start_date")
        end_date = request.GET.get("end_date")
        store_filter = request.GET.get("store")
        campaign_filter = request.GET.get("campaign")

        # Load dataset
        df = load_dataset()

        # Parse numeric columns
        numeric_cols = [
                "requested_qty",
                "unit_cost_price",
                "unit_selling_price",
                "discount",
                "net_extended_line_cost",
                "gross_sales_before_discount",
                "profit_margin"  # <-- use normalized name
            ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Filter by date
        if start_date:
            df = df[df["order_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["order_date"] <= pd.to_datetime(end_date)]

        # Store / Campaign filtering
        # Store / Campaign filtering
        if store_filter:
            df = df[df["store_name"].str.lower() == store_filter.lower()]

        if campaign_filter:
            df = df[df["campaign_channel"].str.lower() == campaign_filter.lower()]


        if df.empty:
            return Response({"message": "No promotion data found for selected filters."}, status=200)

        # ================= PROMOTION METRICS =================
        promo_summary = df.groupby(["marketing_campaign_id", "store_name"]).agg({
                "requested_qty": "sum",
                "gross_sales_before_discount": "sum",
                "net_extended_line_cost": "sum",
                "discount": "mean",
                "profit_margin": "mean"  # <-- use normalized name
            }).reset_index()

        promo_summary = promo_summary.round({
            "discount": 1,
            "profit_margin": 1  # <-- normalized
        })

        # ================= TOP PRODUCTS PER CAMPAIGN =================
        top_products = (
            df.groupby(["marketing_campaign_id", "product_description"])
            .agg({"requested_qty": "sum"})
            .reset_index()
            .sort_values(["marketing_campaign_id", "requested_qty"], ascending=[True, False])
            .groupby("marketing_campaign_id")
            .head(5)  # top 5 products per campaign
        )

        top_products_dict = {}
        for campaign_id, group in top_products.groupby("marketing_campaign_id", group_keys=False):
            top_products_dict[campaign_id] = group[["product_description", "requested_qty"]].to_dict(orient="records")


        # ================= AI INSIGHT CACHING =================
        summary_payload = {
            "promotion_summary": promo_summary.to_dict(orient="records"),
            "top_products": top_products_dict
        }

        cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, "monthly")

        cache.set(cache_key + ":status", {"insight": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_payload, start_date, end_date, None, cache_key, "promotion_analysis")
        ).start()

        return Response({
            "promotion_summary": summary_payload["promotion_summary"],
            "top_products": summary_payload["top_products"],
            "data_key": cache_key
        })

    except Exception as e:
        return Response({"error": f"Failed to compute promotion analytics: {str(e)}"}, status=500)

@api_view(["GET"])
def profit_margin_analytics(request):
    period = request.GET.get("period", "monthly")
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        # Load and normalize dataset
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    # ===== Ensure numeric columns are parsed correctly =====
    numeric_cols = [
        "unit_cost_price",
        "net_extended_line_cost",
        "requested_qty",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ===== Compute Profit & Profit Margin =====
    df["profit"] = df["net_extended_line_cost"] - (df["unit_cost_price"] * df["requested_qty"])
    df["profit_margin"] = df.apply(
        lambda row: (row["profit"] / row["net_extended_line_cost"] * 100) if row["net_extended_line_cost"] else 0,
        axis=1
    )

    today = df["main_date"].max().normalize()

    # ===== Determine current and previous period ranges =====
    if start_date and end_date:
        start_current = pd.to_datetime(start_date)
        end_current = pd.to_datetime(end_date)
        duration = end_current - start_current
        start_previous = start_current - duration - timedelta(days=1)
        end_previous = start_current - timedelta(days=1)

        days = duration.days
        if days <= 7:
            freq = "D"; label_format = "%Y-%m-%d"
        elif days <= 31:
            freq = "W-MON"; label_format = "Week %W"
        elif days <= 365:
            freq = "M"; label_format = "%B"
        else:
            freq = "Q"; label_format = "Q%q %Y"
    else:
        if period == "weekly":
            start_current = today - timedelta(days=today.weekday())
            end_current = start_current + timedelta(days=6)
            start_previous = start_current - timedelta(weeks=1)
            end_previous = start_current - timedelta(days=1)
            freq = "D"; label_format = "%Y-%m-%d"
        elif period == "monthly":
            start_current = today.replace(day=1)
            end_current = (start_current + relativedelta(months=1)) - timedelta(days=1)
            start_previous = start_current - relativedelta(months=1)
            end_previous = start_current - timedelta(days=1)
            freq = "W-MON"; label_format = "Week %W"
        elif period == "yearly":
            start_current = today.replace(month=1, day=1)
            end_current = today.replace(month=12, day=31)
            start_previous = start_current - relativedelta(years=1)
            end_previous = start_current - timedelta(days=1)
            freq = "M"; label_format = "%B"
        else:
            return Response({"error": "Missing or invalid period. Provide either a valid 'period' or both 'start_date' and 'end_date'."}, status=400)

    # ===== Filter current & previous periods =====
    df_current = df[(df["main_date"] >= start_current) & (df["main_date"] <= end_current)]
    df_previous = df[(df["main_date"] >= start_previous) & (df["main_date"] <= end_previous)]

    if df_current.empty:
        return Response({"error": "No profit data found for the current period."}, status=404)
    if df_previous.empty:
        return Response({"error": "No profit data found for the previous period."}, status=404)

    # ===== Summary metrics =====
    profit_current = df_current["profit"].sum()
    revenue_current = df_current["net_extended_line_cost"].sum()
    profit_margin_current = (profit_current / revenue_current * 100) if revenue_current else 0

    profit_previous = df_previous["profit"].sum()
    revenue_previous = df_previous["net_extended_line_cost"].sum()
    profit_margin_previous = (profit_previous / revenue_previous * 100) if revenue_previous else 0

    profit_growth = ((profit_current - profit_previous) / profit_previous * 100) if profit_previous else (100 if profit_current else 0)

    # ===== Breakdown helper =====
    def breakdown(df_slice):
        df_slice = df_slice.copy()
        df_slice["period"] = df_slice["main_date"].dt.to_period(freq).dt.start_time
        summary = df_slice.groupby("period").agg(
            revenue=("net_extended_line_cost", "sum"),
            cost=("unit_cost_price", lambda x: (x * df_slice.loc[x.index, "requested_qty"]).sum()),
            profit=("profit", "sum"),
        ).reset_index()
        summary["label"] = summary["period"].dt.strftime(label_format)
        summary["profit_margin"] = summary.apply(
            lambda row: (row["profit"] / row["revenue"] * 100) if row["revenue"] else 0, axis=1
        )
        return summary.round(2).sort_values("period")

    current_breakdown = breakdown(df_current)
    previous_breakdown = breakdown(df_previous)

    # ===== Cache and AI insight =====
    summary_payload = {
        "period": period or "custom",
        "start_current": str(start_current.date()),
        "end_current": str(end_current.date()),
        "total_profit_current": round(profit_current, 2),
        "total_profit_previous": round(profit_previous, 2),
        "profit_growth_percent": round(profit_growth, 2),
        "profit_margin_current": round(profit_margin_current, 2),
        "profit_margin_previous": round(profit_margin_previous, 2),
        "current_period_breakdown": current_breakdown.to_dict(orient="records"),
        "previous_period_breakdown": previous_breakdown.to_dict(orient="records"),
    }

    cache_key = generate_ai_cache_key(summary_payload, start_current, end_current, period)
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)
    threading.Thread(target=generate_insight_and_forecast_background, args=(
        summary_payload, start_current, end_current, period, cache_key, "profit_margin_analytics"
    )).start()

    return Response({
        **summary_payload,
        "ai_status": "processing",
        "data_key": cache_key
    })

@api_view(['GET'])
def cost_analysis(request):
    try:
        # Load dataset
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    # Ensure numeric columns are parsed
    numeric_cols = ["net_extended_line_cost", "unit_cost_price", "requested_qty"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["main_date"] = pd.to_datetime(df["main_date"], errors="coerce")

    # ===== Query params =====
    period = request.query_params.get("period")
    store_filter = request.query_params.get("store")
    product_filter = request.query_params.get("product")
    start_date_param = request.query_params.get("start_date")
    end_date_param = request.query_params.get("end_date")

    today = pd.Timestamp.today().normalize()
    trend_freq = "W"

    # ===== Determine current & previous period =====
    try:
        if start_date_param and end_date_param:
            start_current = pd.to_datetime(start_date_param)
            end_current = pd.to_datetime(end_date_param)

            if start_current > end_current:
                return Response({"error": "start_date cannot be after end_date."}, status=400)

            delta_days = (end_current - start_current).days
            trend_freq = "D" if delta_days <= 14 else "W" if delta_days <= 60 else "M"

            # ✅ Auto-generate previous period of equal length
            end_previous = start_current - pd.Timedelta(days=1)
            start_previous = end_previous - pd.Timedelta(days=delta_days)

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
            end_current = today + pd.offsets.YearEnd(0)
            start_previous = (start_current - pd.offsets.YearBegin(1)).replace(month=1, day=1)
            end_previous = start_previous + pd.offsets.YearEnd(1)
            trend_freq = "M"

        else:
            return Response({"error": "Provide valid 'period' or 'start_date' and 'end_date'."}, status=400)
    except Exception as e:
        return Response({"error": f"Invalid date input: {str(e)}"}, status=400)


    # ===== Validate date range =====
    min_date = df["main_date"].min()
    max_date = df["main_date"].max()
    if start_current > max_date or end_current < min_date:
        return Response({
            "error": "Provided date range is outside the available data range.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    # ===== Filter current & previous periods =====
    df_current = df[(df["main_date"] >= start_current) & (df["main_date"] <= end_current)].copy()
    df_previous = (
        df[(df["main_date"] >= start_previous) & (df["main_date"] <= end_previous)].copy()
        if start_previous and end_previous else pd.DataFrame(columns=df.columns)
    )

    if df_current.empty:
        return Response({"error": "No data available for the current period."}, status=404)

    # ===== Apply store / product filters =====
    if store_filter:
        df_current = df_current[df_current["store_name"].str.lower() == store_filter.lower()]
    if product_filter:
        df_current = df_current[df_current["product_description"].str.lower() == product_filter.lower()]

    # ===== Metrics =====
    total_cost_current = df_current["net_extended_line_cost"].sum()
    total_cost_previous = df_previous["net_extended_line_cost"].sum() if not df_previous.empty else 0
    growth_percent = ((total_cost_current - total_cost_previous) / total_cost_previous * 100) if total_cost_previous else 0

    # ===== Fix trend_freq for pandas resample (avoid FutureWarning) =====
    trend_freq_map = {"M": "ME", "W": "W", "D": "D"}
    trend_freq = trend_freq_map.get(trend_freq, "W")

    # ===== Trend =====
    trend_current = df_current.set_index("main_date")["net_extended_line_cost"].resample(trend_freq).sum().reset_index()
    trend_current["net_extended_line_cost"] = trend_current["net_extended_line_cost"].round(2)

    if not df_previous.empty:
        trend_previous = df_previous.set_index("main_date")["net_extended_line_cost"].resample(trend_freq).sum().reset_index()
        trend_previous["net_extended_line_cost"] = trend_previous["net_extended_line_cost"].round(2)
    else:
        trend_previous = []

    # ===== Product & Store breakdowns =====
    product_costs = (
        df_current.groupby("product_description")["net_extended_line_cost"]
        .sum().reset_index().rename(columns={"net_extended_line_cost": "total_cost"})
        .sort_values("total_cost", ascending=False)
    )
    product_costs["total_cost"] = product_costs["total_cost"].round(2)

    store_costs = (
        df_current.groupby("store_name")["net_extended_line_cost"]
        .sum().reset_index().rename(columns={"net_extended_line_cost": "total_cost"})
        .sort_values("total_cost", ascending=False)
    )
    store_costs["total_cost"] = store_costs["total_cost"].round(2)

    product_count_per_store = (
        df_current.groupby("store_name")["product_description"]
        .nunique().reset_index().rename(columns={"product_description": "unique_product_count"})
    )

    most_expensive_product = product_costs.iloc[0].to_dict() if not product_costs.empty else {}
    most_expensive_store = store_costs.iloc[0].to_dict() if not store_costs.empty else {}

    # ===== AI Insight / Forecast =====
    summary_payload = {
        "total_cost_current": round(total_cost_current, 2),
        "total_cost_previous": round(total_cost_previous, 2) if not df_previous.empty else None,
        "cost_growth_percent": round(growth_percent, 2) if not df_previous.empty else None,
        "most_expensive_product": most_expensive_product,
        "most_expensive_store": most_expensive_store,
        "product_cost_breakdown": product_costs.to_dict(orient="records"),
        "store_cost_breakdown": store_costs.to_dict(orient="records"),
    }

    cache_key = generate_ai_cache_key(summary_payload, start_current, end_current, period or "custom")
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)
    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, str(start_current.date()), str(end_current.date()), period or "custom", cache_key, "cost_analysis")
    ).start()

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
        "products_involved": sorted(df_current["product_description"].dropna().unique().tolist()),
        "stores_involved": sorted(df_current["store_name"].dropna().unique().tolist()),
        "unique_products_per_store": product_count_per_store.to_dict(orient="records"),
        "most_expensive_product": most_expensive_product,
        "most_expensive_store": most_expensive_store,
        "filters_applied": {
            "store": store_filter,
            "product": product_filter,
            "start_date": start_date_param,
            "end_date": end_date_param
        },
        "data_key": cache_key
    })



'''DEPRECATED'''
@api_view(['GET'])
def list_all_products(request):
    try:
        combined_df, sales_df, invoice_df = load_data()
        df= combined_df

        # Clean and normalize product fields
        df['Product Description'] = df['Product Description'].fillna('').str.strip()
        # df['Product Code'] = df['Product Code'].fillna('').astype(str).str.strip()
        # df['Barcode'] = df['Barcode'].fillna('').astype(str).str.strip()

        # Get unique products
        # products = df[['Product Code', 'Product Description', 'Barcode']].drop_duplicates()
        products = df[['Product Description']].drop_duplicates()
        products = products.sort_values(by='Product Description')

        # Convert to list of dicts
        product_list = products.to_dict(orient='records')

        return Response(product_list, status=status.HTTP_200_OK)

    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



# @api_view(["GET"])
# def invoice_trend_and_conversion(request):
#     """
#     Provides:
#     - Invoice value trend (monthly total invoice value)
#     - Invoice-to-order conversion rate (percentage of orders that were invoiced)
#     """
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         sales_df = pd.read_excel(EXCEL_PATH, sheet_name="salesData")
#         invoice_df = pd.read_excel(EXCEL_PATH, sheet_name="invoiceData")
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     # --- Prepare Dates & Clean Columns ---
#     for df in [sales_df, invoice_df]:
#         df.columns = df.columns.str.strip().str.title()  # Normalize headers
#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#         df["Net Extended Line Cost"] = pd.to_numeric(df.get("Net Extended Line Cost", 0), errors="coerce").fillna(0)
#         df["Requested Qty"] = pd.to_numeric(df.get("Requested Qty", 0), errors="coerce").fillna(0)

#     # --- Filter by Date Range ---
#     if start_date:
#         start_date = pd.to_datetime(start_date)
#         sales_df = sales_df[sales_df["Created Date"] >= start_date]
#         invoice_df = invoice_df[invoice_df["Created Date"] >= start_date]
#     if end_date:
#         end_date = pd.to_datetime(end_date)
#         sales_df = sales_df[sales_df["Created Date"] <= end_date]
#         invoice_df = invoice_df[invoice_df["Created Date"] <= end_date]

#     if sales_df.empty and invoice_df.empty:
#         return Response({"message": "No data available for the selected period."}, status=200)

#     # --- 1️⃣ Invoice Value Trend ---
#     invoice_trend = (
#         invoice_df.groupby(invoice_df["Created Date"].dt.to_period("M"))["Net Extended Line Cost"]
#         .sum()
#         .reset_index()
#     )
#     invoice_trend.columns = ["Month", "Total Invoice Value"]
#     invoice_trend["Month"] = invoice_trend["Month"].astype(str)
#     invoice_trend["Total Invoice Value"] = invoice_trend["Total Invoice Value"].round(2)

#     # --- 2️⃣ Overall Conversion Rate (clean linkage) ---
#     total_orders = sales_df["Order Number"].nunique() if "Order Number" in sales_df.columns else 0
#     total_invoices = invoice_df["Invoice Number"].nunique() if "Invoice Number" in invoice_df.columns else 0

#     # Clean and match order references
#     if "Order Reference" in invoice_df.columns and "Order Number" in sales_df.columns:
#         invoice_df["Order Reference"] = invoice_df["Order Reference"].astype(str).str.strip().str.upper()
#         sales_df["Order Number"] = sales_df["Order Number"].astype(str).str.strip().str.upper()

#         matched_orders = sales_df[sales_df["Order Number"].isin(invoice_df["Order Reference"])]["Order Number"].nunique()
#         conversion_rate = (matched_orders / total_orders * 100) if total_orders > 0 else 0
#     else:
#         matched_orders = 0
#         conversion_rate = 0

#     # --- 3️⃣ Monthly Comparison (Orders vs Invoices) ---
#     order_trend = (
#         sales_df.groupby(sales_df["Created Date"].dt.to_period("M"))["Order Number"]
#         .nunique()
#         .reset_index()
#         .rename(columns={"Order Number": "Unique Orders"})
#     )

#     invoice_count_trend = (
#         invoice_df.groupby(invoice_df["Created Date"].dt.to_period("M"))["Invoice Number"]
#         .nunique()
#         .reset_index()
#         .rename(columns={"Invoice Number": "Unique Invoices"})
#     )

#     trend_comparison = pd.merge(order_trend, invoice_count_trend, on="Created Date", how="outer").fillna(0)
#     trend_comparison["Created Date"] = trend_comparison["Created Date"].astype(str)
#     trend_comparison["Conversion Rate (%)"] = (
#         (trend_comparison["Unique Invoices"] / trend_comparison["Unique Orders"].replace(0, pd.NA)) * 100
#     ).round(2)

#     # --- Final Summary ---
#     summary = {
#         "total_orders": int(total_orders),
#         "total_invoices": int(total_invoices),
#         "invoiced_orders": int(matched_orders),
#         "uninvoiced_orders": int(total_orders - matched_orders),
#         "overall_conversion_rate (%)": round(conversion_rate, 2),
#         "period_start": str(start_date.date()) if start_date else None,
#         "period_end": str(end_date.date()) if end_date else None,
#     }

#     return Response({
#         "invoice_value_trend": invoice_trend.to_dict(orient="records"),
#         "monthly_order_invoice_comparison": trend_comparison.to_dict(orient="records"),
#         "summary": summary
#     })
# @api_view(["GET"])
# def invoice_trend_and_conversion(request):
#     """
#     Provides:
#     - Invoice value trend (monthly total invoice value)
#     - Invoice-to-order conversion rate (percentage of orders that were invoiced)
#     - AI insight and forecast (background)
#     """
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         sales_df = pd.read_excel(EXCEL_PATH, sheet_name="salesData")
#         invoice_df = pd.read_excel(EXCEL_PATH, sheet_name="invoiceData")
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     # --- Prepare Dates & Clean Columns ---
#     for df in [sales_df, invoice_df]:
#         df.columns = df.columns.str.strip().str.title()  # Normalize headers
#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#         df["Net Extended Line Cost"] = pd.to_numeric(df.get("Net Extended Line Cost", 0), errors="coerce").fillna(0)
#         df["Requested Qty"] = pd.to_numeric(df.get("Requested Qty", 0), errors="coerce").fillna(0)

#     # --- Filter by Date Range ---
#     if start_date:
#         start_date = pd.to_datetime(start_date)
#         sales_df = sales_df[sales_df["Created Date"] >= start_date]
#         invoice_df = invoice_df[invoice_df["Created Date"] >= start_date]
#     if end_date:
#         end_date = pd.to_datetime(end_date)
#         sales_df = sales_df[sales_df["Created Date"] <= end_date]
#         invoice_df = invoice_df[invoice_df["Created Date"] <= end_date]

#     if sales_df.empty and invoice_df.empty:
#         return Response({"message": "No data available for the selected period."}, status=200)

#     # --- 1️⃣ Invoice Value Trend ---
#     invoice_trend = (
#         invoice_df.groupby(invoice_df["Created Date"].dt.to_period("M"))["Net Extended Line Cost"]
#         .sum()
#         .reset_index()
#     )
#     invoice_trend.columns = ["Month", "Total Invoice Value"]
#     invoice_trend["Month"] = invoice_trend["Month"].astype(str)
#     invoice_trend["Total Invoice Value"] = invoice_trend["Total Invoice Value"].round(2)

#     # --- 2️⃣ Conversion Rate ---
#     total_orders = sales_df["Order Number"].nunique() if "Order Number" in sales_df.columns else 0
#     total_invoices = invoice_df["Invoice Number"].nunique() if "Invoice Number" in invoice_df.columns else 0

#     if "Order Reference" in invoice_df.columns and "Order Number" in sales_df.columns:
#         invoice_df["Order Reference"] = invoice_df["Order Reference"].astype(str).str.strip().str.upper()
#         sales_df["Order Number"] = sales_df["Order Number"].astype(str).str.strip().str.upper()
#         matched_orders = sales_df[sales_df["Order Number"].isin(invoice_df["Order Reference"])]["Order Number"].nunique()
#         conversion_rate = (matched_orders / total_orders * 100) if total_orders > 0 else 0
#     else:
#         matched_orders = 0
#         conversion_rate = 0

#     # --- 3️⃣ Monthly Comparison ---
#     order_trend = (
#         sales_df.groupby(sales_df["Created Date"].dt.to_period("M"))["Order Number"]
#         .nunique()
#         .reset_index()
#         .rename(columns={"Order Number": "Unique Orders"})
#     )
#     invoice_count_trend = (
#         invoice_df.groupby(invoice_df["Created Date"].dt.to_period("M"))["Invoice Number"]
#         .nunique()
#         .reset_index()
#         .rename(columns={"Invoice Number": "Unique Invoices"})
#     )
#     trend_comparison = pd.merge(order_trend, invoice_count_trend, on="Created Date", how="outer").fillna(0)
#     trend_comparison["Created Date"] = trend_comparison["Created Date"].astype(str)
#     trend_comparison["Conversion Rate (%)"] = (
#         (trend_comparison["Unique Invoices"] / trend_comparison["Unique Orders"].replace(0, pd.NA)) * 100
#     ).round(2)

#     # --- 4️⃣ Summary ---
#     summary = {
#         "total_orders": int(total_orders),
#         "total_invoices": int(total_invoices),
#         "invoiced_orders": int(matched_orders),
#         "uninvoiced_orders": int(total_orders - matched_orders),
#         "overall_conversion_rate (%)": round(conversion_rate, 2),
#         "period_start": str(start_date.date()) if start_date else None,
#         "period_end": str(end_date.date()) if end_date else None,
#     }

#     # --- 5️⃣ Background AI Task ---
#     cache_key = f"invoice_trend_and_conversion:{start_date or 'null'}:{end_date or 'null'}"
#     ai_summary = {
#         "summary": summary,
#         "invoice_value_trend": invoice_trend.to_dict(orient="records")[:12],
#         "monthly_comparison": trend_comparison.to_dict(orient="records")[:12],
#     }

#     cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
#     threading.Thread(
#         target=generate_insight_and_forecast_background,
#         args=(ai_summary, start_date, end_date, "month", cache_key, "invoice_trend_and_conversion"),
#     ).start()

#     # --- Final Cleanup ---
#     invoice_trend = invoice_trend.replace([np.inf, -np.inf, np.nan], 0)
#     trend_comparison = trend_comparison.replace([np.inf, -np.inf, np.nan], 0)

#     # --- 6️⃣ Response ---
#     return Response({
#         "invoice_value_trend": invoice_trend.to_dict(orient="records"),
#         "monthly_order_invoice_comparison": trend_comparison.to_dict(orient="records"),
#         "summary": summary,
#         "insight_status": "processing",
#         "data_key": cache_key
#     })

# @api_view(["GET"])
# def product_performance_analysis(request):
#     """
#     Provides product-level analytics across merged sales (Order) and invoice data.
#     - Profitability (Revenue, Cost, Margin)
#     - Demand trend
#     - Return/refund rate (if present)
#     - Inventory turnover and stock days (if columns exist)
#     """
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         combined_df, sales_df, invoice_df = load_data()
#         df= combined_df
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     # --- Data Preparation ---
#     df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#     df["Net Extended Line Cost"] = pd.to_numeric(df.get("Net Extended Line Cost", 0), errors="coerce").fillna(0)
#     df["Cost Price"] = pd.to_numeric(df.get("Cost Price", 0), errors="coerce").fillna(0)
#     df["Requested Qty"] = pd.to_numeric(df.get("Requested Qty", 0), errors="coerce").fillna(0)

#     for col in ["Returned Qty", "Refund Amount"]:
#         if col not in df.columns:
#             df[col] = 0
#         else:
#             df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

#     df = df.dropna(subset=["Created Date"])

#     # --- Filter by Date Range ---
#     try:
#         if start_date:
#             df = df[df["Created Date"] >= pd.to_datetime(start_date)]
#         if end_date:
#             df = df[df["Created Date"] <= pd.to_datetime(end_date)]
#     except Exception as e:
#         return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No data found in the selected period."}, status=200)

#     # --- Calculations ---
#     df["Revenue"] = df["Net Extended Line Cost"]
#     df["Cost"] = df["Requested Qty"] * df["Cost Price"]
#     df["Profit"] = df["Revenue"] - df["Cost"]
#     df["Profit Margin (%)"] = (df["Profit"] / df["Revenue"].replace(0, pd.NA)) * 100
#     df["Return Rate (%)"] = (df["Returned Qty"] / df["Requested Qty"].replace(0, pd.NA)) * 100
#     df["Refund Rate (%)"] = (df["Refund Amount"] / df["Revenue"].replace(0, pd.NA)) * 100

#     # --- Product-Level Summary ---
#     summary = (
#         df.groupby("Product Description")
#         .agg({
#             "Requested Qty": "sum",
#             "Revenue": "sum",
#             "Profit": "sum",
#             "Profit Margin (%)": "mean",
#             "Returned Qty": "sum",
#             "Return Rate (%)": "mean",
#             "Refund Amount": "sum",
#             "Refund Rate (%)": "mean",
#         })
#         .sort_values(by="Profit", ascending=False)
#         .round(2)
#         .reset_index()
#     )

#     # --- Inventory Turnover & Stock Days (optional) ---
#     inventory_summary = {}
#     if "Opening Stock" in df.columns and "Closing Stock" in df.columns:
#         df["Opening Stock"] = pd.to_numeric(df["Opening Stock"], errors="coerce").fillna(0)
#         df["Closing Stock"] = pd.to_numeric(df["Closing Stock"], errors="coerce").fillna(0)
#         df["Average Inventory"] = (df["Opening Stock"] + df["Closing Stock"]) / 2
#         df["Inventory Turnover"] = df["Cost"] / df["Average Inventory"].replace(0, pd.NA)
#         df["Stock Days"] = 365 / df["Inventory Turnover"].replace(0, pd.NA)
#         inventory_summary = (
#             df.groupby("Product Description")[["Inventory Turnover", "Stock Days"]]
#             .mean().round(2).to_dict(orient="index")
#         )

#     # --- Demand Trend ---
#     demand_trend = (
#         df.groupby(df["Created Date"].dt.to_period("M"))["Requested Qty"]
#         .sum().reset_index()
#     )
#     demand_trend.columns = ["Month", "Total Demand"]
#     demand_trend["Month"] = demand_trend["Month"].astype(str)

#     # --- AI Background Task ---
#     cache_key = f"product_performance_analysis:{start_date or 'null'}:{end_date or 'null'}"
#     ai_summary = {
#         "top_products": summary.head(5).to_dict(orient="records"),
#         "average_profit_margin": round(summary["Profit Margin (%)"].mean(), 2),
#         "average_return_rate": round(summary["Return Rate (%)"].mean(), 2),
#         "average_refund_rate": round(summary["Refund Rate (%)"].mean(), 2),
#         "inventory_metrics": inventory_summary,
#     }

#     cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
#     threading.Thread(
#         target=generate_insight_and_forecast_background,
#         args=("product_performance_analysis", ai_summary, start_date, end_date, "month", cache_key),
#     ).start()

#     # --- Cleanup NaN/Inf ---
#     summary = summary.replace([np.inf, -np.inf, np.nan], 0)
#     demand_trend = demand_trend.replace([np.inf, -np.inf, np.nan], 0)
#     for key in inventory_summary.keys():
#         for metric in inventory_summary[key]:
#             val = inventory_summary[key][metric]
#             if pd.isna(val) or np.isinf(val):
#                 inventory_summary[key][metric] = 0

#     # --- Response ---
#     return Response({
#         "product_performance_summary": summary.to_dict(orient="records"),
#         "inventory_summary": inventory_summary,
#         "demand_trend": demand_trend.to_dict(orient="records"),
#         "insight_status": "processing",
#         "data_key": cache_key
#     })


@api_view(["GET"])
def operations_metrics(request):
    try:
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    # === Normalize numeric fields ===
    numeric_cols = [
        "net_extended_line_cost", "gross_sales_before_discount", "profit_margin",
        "lead_time_days", "logistics_cost_per_unit", "operational_cost_total",
        "inventory_level_before_sale", "warehouse_utilization",
        "employee_attendance__month", "supplier_rating"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # === Date filters ===
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    region = request.GET.get("region")
    store = request.GET.get("store")
    campaign = request.GET.get("campaign")

    df["main_date"] = pd.to_datetime(df["main_date"], errors="coerce")
    today = pd.Timestamp.today().normalize()

    if start_date and end_date:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
    else:
        start_date = today.replace(day=1)
        end_date = today

    # === Apply filters ===
    df = df[(df["main_date"] >= start_date) & (df["main_date"] <= end_date)]
    if region:
        df = df[df["region"].str.lower() == region.lower()]
    if store:
        df = df[df["store_name"].str.lower() == store.lower()]
    if campaign:
        df = df[df["campaign_channel"].str.lower() == campaign.lower()]

    if df.empty:
        return Response({"message": "No data found for selected filters."}, status=404)

    # === 1️⃣ Sales & Revenue Metrics ===
    total_sales = df["gross_sales_before_discount"].sum()
    total_cost = df["net_extended_line_cost"].sum()
    total_profit = total_sales - total_cost
    avg_margin = df["profit_margin"].mean()

    # === 2️⃣ Logistics Metrics ===
    delivery_status = df["delivery_status"].value_counts(normalize=True) * 100
    logistics_metrics = {
        "average_lead_time_days": round(df["lead_time_days"].mean(), 2),
        "average_logistics_cost_per_unit": round(df["logistics_cost_per_unit"].mean(), 2),
        "delivery_status_breakdown": delivery_status.to_dict()
    }

    # === 3️⃣ Inventory & Supply Chain ===
    inventory_metrics = {
        "average_inventory_level": round(df["inventory_level_before_sale"].mean(), 2),
        "average_warehouse_utilization": round(df["warehouse_utilization"].mean(), 2),
        "stock_replenishment_rate": round(df["stock_replenishment_date"].notna().mean() * 100, 2)
    }

    # === 4️⃣ Customer Experience ===
    customer_metrics = {
        "average_csat": round(df["customer_satisfaction_csat"].mean(), 2),
        "frequent_buyers": round((df["customer_segment"].str.contains("Frequent", case=False, na=False)).mean() * 100, 2),
        "feedback_sentiment_summary": {
            "positive": round((df["feedback_comment"].str.contains("good|great|excellent", case=False, na=False)).mean() * 100, 2),
            "neutral": round((df["feedback_comment"].str.contains("average|ok", case=False, na=False)).mean() * 100, 2),
            "negative": round((df["feedback_comment"].str.contains("bad|poor|slow", case=False, na=False)).mean() * 100, 2)
        }
    }

    # === 5️⃣ Workforce & Employee Performance ===
    workforce_metrics = {
        "average_attendance": round(df["employee_attendance__month"].mean(), 2),
        "top_performers": df.groupby("salesperson_id")["gross_sales_before_discount"]
                            .sum().nlargest(5).index.tolist()
    }

    # === 6️⃣ Marketing & Supplier Metrics ===
    marketing_metrics = {
        "total_campaigns": df["marketing_campaign_id"].nunique(),
        "top_channels": (df["campaign_channel"].value_counts(normalize=True) * 100).round(2).to_dict(),
        "average_supplier_rating": round(df["supplier_rating"].mean(), 2)
    }

    # === Summary payload ===
    summary_payload = {
        "sales": {
            "total_sales": round(total_sales, 2),
            "total_cost": round(total_cost, 2),
            "total_profit": round(total_profit, 2),
            "average_profit_margin": round(avg_margin, 2)
        },
        "logistics": logistics_metrics,
        "inventory": inventory_metrics,
        "customer": customer_metrics,
        "workforce": workforce_metrics,
        "marketing": marketing_metrics
    }

    # === AI Insight + Forecast ===
    cache_key = generate_ai_cache_key(summary_payload, start_date, end_date, "operations")
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, str(start_date.date()), str(end_date.date()), "operations", cache_key, "operations_metrics")
    ).start()

    return Response({
        "period": "custom",
        "start_date": start_date.date(),
        "end_date": end_date.date(),
        "sales_metrics": summary_payload["sales"],
        "logistics_metrics": summary_payload["logistics"],
        "inventory_metrics": summary_payload["inventory"],
        "customer_metrics": summary_payload["customer"],
        "workforce_metrics": summary_payload["workforce"],
        "marketing_metrics": summary_payload["marketing"],
        "data_key": cache_key,
        "ai_status": "processing"
    })

@api_view(["GET"])
def finance_analytics(request):
    """
    Comprehensive finance analytics for large retail operations.
    Computes profitability, efficiency, and cashflow metrics,
    includes AI insight/forecast, past-period variance, and financial ratios.
    """
    try:
        df = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    start_date_q = request.GET.get("start_date")
    end_date_q = request.GET.get("end_date")
    region_q = request.GET.get("region")
    channel_q = request.GET.get("channel")

    if "main_date" not in df.columns:
        return Response({"error": "Dataset missing 'main_date' column."}, status=500)

    # Normalize and coerce numeric columns
    def coerce_num(col, default=0):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
        else:
            df[col] = default

    for col in [
        "gross_sales_before_discount", "net_extended_line_cost", "profit_margin",
        "discount", "requested_qty", "operational_cost_total", "logistics_cost_per_unit",
        "inventory_level_before_sale", "forecasted_demand_next_period", "actual_demand",
        "lead_time_days"
    ]:
        coerce_num(col)

    # Date handling
    df["main_date"] = pd.to_datetime(df["main_date"], errors="coerce")
    df["order_date"] = pd.to_datetime(df.get("order_date", df["main_date"]), errors="coerce")
    df["delivery_date"] = pd.to_datetime(df.get("delivery_date", pd.NaT), errors="coerce")

    # Parse input date filters
    try:
        start_dt = pd.to_datetime(start_date_q) if start_date_q else df["main_date"].min()
        end_dt = pd.to_datetime(end_date_q) if end_date_q else df["main_date"].max()
    except Exception as e:
        return Response({"error": f"Invalid date inputs: {str(e)}"}, status=400)

    # Work from unfiltered copy for historical comparison
    df_full = df.copy()

    # Apply filters for current period
    df_current = df_full[(df_full["main_date"] >= start_dt) & (df_full["main_date"] <= end_dt)]
    if region_q and "region" in df_current.columns:
        df_current = df_current[df_current["region"].str.contains(region_q, case=False, na=False)]
    if channel_q and "channel" in df_current.columns:
        df_current = df_current[df_current["channel"].str.contains(channel_q, case=False, na=False)]
    if df_current.empty:
        return Response({"message": "No data found for selected filters."}, status=200)

    # === Define previous-period window (same length before start_dt) ===
    period_length = (end_dt - start_dt).days or 1
    prev_start = start_dt - timedelta(days=period_length)
    prev_end = start_dt - timedelta(days=1)
    df_previous = df_full[(df_full["main_date"] >= prev_start) & (df_full["main_date"] <= prev_end)]

    # === Helper for summary computation ===
    def compute_summary(data):
        total_revenue = data["gross_sales_before_discount"].sum()
        total_cogs = data["net_extended_line_cost"].sum()
        gross_profit = total_revenue - total_cogs
        operating_expenses = data["operational_cost_total"].sum()
        logistics_cost = data["logistics_cost_per_unit"].sum()
        operating_income = gross_profit - operating_expenses - logistics_cost
        ebitda_proxy = operating_income + (operating_expenses * 0.25)  # simplified
        gross_margin = (gross_profit / total_revenue * 100) if total_revenue else 0
        operating_margin = (operating_income / total_revenue * 100) if total_revenue else 0
        ebitda_margin = (ebitda_proxy / total_revenue * 100) if total_revenue else 0

        return {
            "revenue": round(total_revenue, 2),
            "cogs": round(total_cogs, 2),
            "gross_profit": round(gross_profit, 2),
            "operating_expenses": round(operating_expenses, 2),
            "logistics_cost": round(logistics_cost, 2),
            "operating_income": round(operating_income, 2),
            "ebitda_proxy": round(ebitda_proxy, 2),
            "gross_margin": round(gross_margin, 2),
            "operating_margin": round(operating_margin, 2),
            "ebitda_margin": round(ebitda_margin, 2),
        }

    summary_current = compute_summary(df_current)
    summary_previous = compute_summary(df_previous)

    # === Variance calculations ===
    def pct_variance(current, previous):
        if previous == 0:
            return None
        return round(((current - previous) / previous) * 100, 2)

    variance = {
        f"{k}_variance_%": pct_variance(summary_current[k], summary_previous[k])
        for k in summary_current
    }

    # === Profitability breakdown ===
    profit_by_region = df_current.groupby("region")["gross_sales_before_discount"].sum().sub(
        df_current.groupby("region")["net_extended_line_cost"].sum(), fill_value=0).round(2).to_dict() if "region" in df_current.columns else {}
    profit_by_category = df_current.groupby("product_category")["gross_sales_before_discount"].sum().sub(
        df_current.groupby("product_category")["net_extended_line_cost"].sum(), fill_value=0).round(2).to_dict() if "product_category" in df_current.columns else {}
    profit_by_channel = df_current.groupby("channel")["gross_sales_before_discount"].sum().sub(
        df_current.groupby("channel")["net_extended_line_cost"].sum(), fill_value=0).round(2).to_dict() if "channel" in df_current.columns else {}
    profit_by_supplier = df_current.groupby("supplier_name")["gross_sales_before_discount"].sum().sub(
        df_current.groupby("supplier_name")["net_extended_line_cost"].sum(), fill_value=0).round(2).to_dict() if "supplier_name" in df_current.columns else {}

    # === Efficiency metrics ===
    avg_operational_cost = round(float(df_current["operational_cost_total"].mean() or 0), 2)
    avg_logistics_cost_per_unit = round(float(df_current["logistics_cost_per_unit"].mean() or 0), 2)
    avg_inventory = float(df_current["inventory_level_before_sale"].mean() or 0)
    inventory_turnover = round(summary_current["cogs"] / avg_inventory, 2) if avg_inventory else None
    avg_lead_time = round(float(df_current["lead_time_days"].mean() or 0), 1)
    df_current["days_to_delivery"] = (df_current["delivery_date"] - df_current["order_date"]).dt.days
    avg_days_to_delivery = round(float(df_current["days_to_delivery"].mean() or 0), 1)

    # === Cashflow ===
    revenue_by_payment = df_current.groupby(df_current["payment_method"].fillna("UNKNOWN"))["gross_sales_before_discount"].sum().round(2).to_dict()

    # === Timeline granularity ===
    trend_period = "D" if (end_dt - start_dt).days <= 45 else "W"
    timeline = (
        df_current.set_index("main_date")
        .resample(trend_period)[["gross_sales_before_discount", "net_extended_line_cost"]]
        .sum()
        .assign(profit=lambda x: x["gross_sales_before_discount"] - x["net_extended_line_cost"])
        .reset_index()
    )
    timeline["period"] = timeline["main_date"].dt.strftime("%Y-%m-%d" if trend_period == "D" else "W%U")
    trends = timeline.to_dict(orient="records")

    # === Final Payload ===
    summary_payload = {
        "summary_current": summary_current,
        "summary_previous": summary_previous,
        "variance": variance,
        "profitability": {
            "by_region": profit_by_region,
            "by_category": profit_by_category,
            "by_channel": profit_by_channel,
            "by_supplier": profit_by_supplier,
        },
        "efficiency": {
            "avg_operational_cost": avg_operational_cost,
            "avg_logistics_cost_per_unit": avg_logistics_cost_per_unit,
            "inventory_turnover": inventory_turnover,
            "avg_lead_time_days": avg_lead_time,
            "avg_days_to_delivery": avg_days_to_delivery,
        },
        "cashflow": {
            "revenue_by_payment_method": revenue_by_payment,
        },
        "trends": trends,
    }

    # === AI insight + forecast ===
    cache_key = generate_ai_cache_key(summary_payload, start_dt, end_dt, "operations")
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, str(start_dt.date()), str(end_dt.date()), "operations", cache_key, "operations_metrics"),
        daemon=True,
    ).start()

    return Response({
        **summary_payload,
        "data_key": cache_key,
        "ai_status": "processing"
    })


# @api_view(["GET"])
# def customer_experience_analytics(request):
#     """
#     Customer Experience (CX) Analytics:
#     Focuses purely on customer satisfaction and experience — 
#     including CSAT, feedback sentiment, lead time, on-time delivery, 
#     campaign effectiveness, and satisfaction trends.
#     """
#     try:
#         df = load_dataset()
#     except Exception as e:
#         return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

#     start_date_q = request.GET.get("start_date")
#     end_date_q = request.GET.get("end_date")
#     region_q = request.GET.get("region")
#     channel_q = request.GET.get("channel")

#     if "main_date" not in df.columns:
#         return Response({"error": "Dataset missing 'main_date' column."}, status=500)

#     # Normalize key numeric and text columns
#     num_cols = ["customer_satisfaction_csat", "lead_time_days"]
#     for col in num_cols:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

#     df["main_date"] = pd.to_datetime(df["main_date"], errors="coerce")
#     df["order_date"] = pd.to_datetime(df.get("order_date", df["main_date"]), errors="coerce")
#     df["delivery_date"] = pd.to_datetime(df.get("delivery_date", pd.NaT), errors="coerce")

#     # === Date filtering ===
#     try:
#         start_dt = pd.to_datetime(start_date_q) if start_date_q else df["main_date"].min()
#         end_dt = pd.to_datetime(end_date_q) if end_date_q else df["main_date"].max()
#     except Exception as e:
#         return Response({"error": f"Invalid date inputs: {str(e)}"}, status=400)

#     df = df[(df["main_date"] >= start_dt) & (df["main_date"] <= end_dt)]
#     if region_q and "region" in df.columns:
#         df = df[df["region"].str.contains(region_q, case=False, na=False)]
#     if channel_q and "channel" in df.columns:
#         df = df[df["channel"].str.contains(channel_q, case=False, na=False)]

#     if df.empty:
#         return Response({"message": "No data found for selected filters."}, status=200)

#     # === Core CX metrics ===
#     avg_csat = round(df["customer_satisfaction_csat"].mean(), 2)
#     feedback_count = len(df["feedback_comment"].dropna())

#     positive_feedback = df["feedback_comment"].str.contains(
#         "good|great|excellent|fast|satisfied|happy", case=False, na=False
#     ).sum()
#     negative_feedback = df["feedback_comment"].str.contains(
#         "bad|slow|poor|late|disappointed|unhappy", case=False, na=False
#     ).sum()

#     positive_pct = round((positive_feedback / feedback_count) * 100, 1) if feedback_count else 0
#     negative_pct = round((negative_feedback / feedback_count) * 100, 1) if feedback_count else 0

#     # === Service delivery ===
#     avg_lead_time = round(df["lead_time_days"].mean(), 1)
#     on_time_delivery_rate = round(
#         (df["delivery_status"].str.contains("Delivered", case=False, na=False).sum() / len(df)) * 100, 1
#     )
#     avg_days_to_delivery = (df["delivery_date"] - df["order_date"]).dt.days.mean()
#     avg_days_to_delivery = round(avg_days_to_delivery or 0, 1)

#     # === Marketing effectiveness ===
#     top_campaigns = (
#         df.groupby("campaign_channel")["customer_satisfaction_csat"].mean()
#         .round(2)
#         .sort_values(ascending=False)
#         .head(5)
#         .to_dict()
#         if "campaign_channel" in df.columns
#         else {}
#     )

#     # === Breakdown by key customer dimensions ===
#     csat_by_region = (
#         df.groupby("region")["customer_satisfaction_csat"].mean().round(2).to_dict()
#         if "region" in df.columns
#         else {}
#     )
#     csat_by_channel = (
#         df.groupby("channel")["customer_satisfaction_csat"].mean().round(2).to_dict()
#         if "channel" in df.columns
#         else {}
#     )
#     csat_by_segment = (
#         df.groupby("customer_segment")["customer_satisfaction_csat"].mean().round(2).to_dict()
#         if "customer_segment" in df.columns
#         else {}
#     )
#     csat_by_category = (
#         df.groupby("product_category")["customer_satisfaction_csat"].mean().round(2).to_dict()
#         if "product_category" in df.columns
#         else {}
#     )

#     # === Previous period comparison ===
#     period_length = (end_dt - start_dt).days or 1
#     prev_start = start_dt - timedelta(days=period_length)
#     prev_end = start_dt - timedelta(days=1)
#     df_prev = df[(df["main_date"] >= prev_start) & (df["main_date"] <= prev_end)]

#     avg_csat_prev = df_prev["customer_satisfaction_csat"].mean() if not df_prev.empty else 0
#     avg_lead_time_prev = df_prev["lead_time_days"].mean() if not df_prev.empty else 0
#     on_time_prev = (
#         df_prev["delivery_status"].str.contains("Delivered", case=False, na=False).sum() / len(df_prev) * 100
#         if len(df_prev) > 0
#         else 0
#     )

#     def pct_variance(curr, prev):
#         if not prev:
#             return None
#         return round(((curr - prev) / prev) * 100, 2)

#     variance = {
#         "csat_variance_%": pct_variance(avg_csat, avg_csat_prev),
#         "lead_time_variance_%": pct_variance(avg_lead_time, avg_lead_time_prev),
#         "on_time_delivery_variance_%": pct_variance(on_time_delivery_rate, on_time_prev),
#     }

#     # === Trend data ===
#     trend_period = "D" if (end_dt - start_dt).days <= 45 else "W"
#     timeline = (
#         df.set_index("main_date")
#         .resample(trend_period)["customer_satisfaction_csat"]
#         .mean()
#         .reset_index()
#     )
#     timeline["period"] = timeline["main_date"].dt.strftime("%Y-%m-%d" if trend_period == "D" else "W%U")
#     trends = timeline.to_dict(orient="records")

#     # === Final Payload ===
#     cx_payload = {
#         "summary_current": {
#             "avg_csat": avg_csat,
#             "positive_feedback_%": positive_pct,
#             "negative_feedback_%": negative_pct,
#             "avg_lead_time_days": avg_lead_time,
#             "on_time_delivery_rate_%": on_time_delivery_rate,
#             "avg_days_to_delivery": avg_days_to_delivery,
#         },
#         "summary_previous": {
#             "avg_csat": round(avg_csat_prev, 2),
#             "avg_lead_time_days": round(avg_lead_time_prev, 1),
#             "on_time_delivery_rate_%": round(on_time_prev, 1),
#         },
#         "variance": variance,
#         "breakdowns": {
#             "csat_by_region": csat_by_region,
#             "csat_by_channel": csat_by_channel,
#             "csat_by_segment": csat_by_segment,
#             "csat_by_category": csat_by_category,
#         },
#         "marketing_effectiveness": {"top_campaign_channels": top_campaigns},
#         "trends": trends,
#     }

#     # === AI insight + forecast ===
#     cache_key = generate_ai_cache_key(cx_payload, start_dt, end_dt, "customer_experience")
#     cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)

#     threading.Thread(
#         target=generate_insight_and_forecast_background,
#         args=(cx_payload, str(start_dt.date()), str(end_dt.date()), "customer_experience_analytics", cache_key, "cx_metrics"),
#         daemon=True,
#     ).start()

#     return Response({**cx_payload, "data_key": cache_key, "ai_status": "processing"})

@api_view(["GET"])
def customer_experience_analytics(request):
    """
    Customer Experience (CX) Analytics:
    Focuses purely on customer satisfaction and experience — 
    including CSAT, feedback sentiment, lead time, on-time delivery, 
    campaign effectiveness, and satisfaction trends.
    """
    try:
        df_full = load_dataset()
    except Exception as e:
        return Response({"error": f"Failed to load dataset: {str(e)}"}, status=500)

    start_date_q = request.GET.get("start_date")
    end_date_q = request.GET.get("end_date")
    region_q = request.GET.get("region")
    channel_q = request.GET.get("channel")

    if "main_date" not in df_full.columns:
        return Response({"error": "Dataset missing 'main_date' column."}, status=500)

    # Normalize key numeric and text columns
    df_full["main_date"] = pd.to_datetime(df_full["main_date"], errors="coerce")
    df_full["order_date"] = pd.to_datetime(df_full.get("order_date", df_full["main_date"]), errors="coerce")
    df_full["delivery_date"] = pd.to_datetime(df_full.get("delivery_date", pd.NaT), errors="coerce")

    num_cols = ["customer_satisfaction_csat", "lead_time_days"]
    for col in num_cols:
        if col in df_full.columns:
            df_full[col] = pd.to_numeric(df_full[col], errors="coerce").fillna(0)

    # === Date filtering ===
    try:
        start_dt = pd.to_datetime(start_date_q) if start_date_q else df_full["main_date"].min()
        end_dt = pd.to_datetime(end_date_q) if end_date_q else df_full["main_date"].max()
    except Exception as e:
        return Response({"error": f"Invalid date inputs: {str(e)}"}, status=400)

    df = df_full[(df_full["main_date"] >= start_dt) & (df_full["main_date"] <= end_dt)]
    if region_q and "region" in df.columns:
        df = df[df["region"].str.contains(region_q, case=False, na=False)]
    if channel_q and "channel" in df.columns:
        df = df[df["channel"].str.contains(channel_q, case=False, na=False)]

    if df.empty:
        return Response({"message": "No data found for selected filters."}, status=200)

    # === Core CX metrics ===
    avg_csat = round(df["customer_satisfaction_csat"].mean(), 2)
    feedback_count = len(df["feedback_comment"].dropna())

    positive_feedback = df["feedback_comment"].str.contains(
        "good|great|excellent|fast|satisfied|happy", case=False, na=False
    ).sum()
    negative_feedback = df["feedback_comment"].str.contains(
        "bad|slow|poor|late|disappointed|unhappy", case=False, na=False
    ).sum()

    positive_pct = round((positive_feedback / feedback_count) * 100, 1) if feedback_count else 0
    negative_pct = round((negative_feedback / feedback_count) * 100, 1) if feedback_count else 0

    # === Service delivery ===
    avg_lead_time = round(df["lead_time_days"].mean(), 1)
    on_time_delivery_rate = round(
        (df["delivery_status"].str.contains("Delivered", case=False, na=False).sum() / len(df)) * 100, 1
    )
    avg_days_to_delivery = (df["delivery_date"] - df["order_date"]).dt.days.mean()
    avg_days_to_delivery = round(avg_days_to_delivery or 0, 1)

    # === Marketing effectiveness ===
    top_campaigns = (
        df.groupby("campaign_channel")["customer_satisfaction_csat"].mean()
        .round(2)
        .sort_values(ascending=False)
        .head(5)
        .to_dict()
        if "campaign_channel" in df.columns
        else {}
    )

    # === Breakdown by key customer dimensions ===
    csat_by_region = (
        df.groupby("region")["customer_satisfaction_csat"].mean().round(2).to_dict()
        if "region" in df.columns
        else {}
    )
    csat_by_channel = (
        df.groupby("channel")["customer_satisfaction_csat"].mean().round(2).to_dict()
        if "channel" in df.columns
        else {}
    )
    csat_by_segment = (
        df.groupby("customer_segment")["customer_satisfaction_csat"].mean().round(2).to_dict()
        if "customer_segment" in df.columns
        else {}
    )
    csat_by_category = (
        df.groupby("product_category")["customer_satisfaction_csat"].mean().round(2).to_dict()
        if "product_category" in df.columns
        else {}
    )

    # === Previous period comparison (fixed) ===
    period_length = (end_dt - start_dt).days or 1
    prev_start = start_dt - timedelta(days=period_length)
    prev_end = start_dt - timedelta(days=1)
    df_prev = df_full[(df_full["main_date"] >= prev_start) & (df_full["main_date"] <= prev_end)]

    avg_csat_prev = df_prev["customer_satisfaction_csat"].mean() if not df_prev.empty else 0
    avg_lead_time_prev = df_prev["lead_time_days"].mean() if not df_prev.empty else 0
    on_time_prev = (
        df_prev["delivery_status"].str.contains("Delivered", case=False, na=False).sum() / len(df_prev) * 100
        if len(df_prev) > 0
        else 0
    )

    def pct_variance(curr, prev):
        if prev == 0 or prev is None:
            return None
        return round(((curr - prev) / prev) * 100, 2)

    variance = {
        "csat_variance_%": pct_variance(avg_csat, avg_csat_prev),
        "lead_time_variance_%": pct_variance(avg_lead_time, avg_lead_time_prev),
        "on_time_delivery_variance_%": pct_variance(on_time_delivery_rate, on_time_prev),
    }

    # === Trend data ===
    trend_period = "D" if (end_dt - start_dt).days <= 45 else "W"
    timeline = (
        df.set_index("main_date")
        .resample(trend_period)["customer_satisfaction_csat"]
        .mean()
        .reset_index()
    )
    timeline["period"] = timeline["main_date"].dt.strftime("%Y-%m-%d" if trend_period == "D" else "W%U")
    trends = timeline.to_dict(orient="records")

    # === Final Payload ===
    cx_payload = {
        "summary_current": {
            "avg_csat": avg_csat,
            "positive_feedback_%": positive_pct,
            "negative_feedback_%": negative_pct,
            "avg_lead_time_days": avg_lead_time,
            "on_time_delivery_rate_%": on_time_delivery_rate,
            "avg_days_to_delivery": avg_days_to_delivery,
        },
        "summary_previous": {
            "avg_csat": round(avg_csat_prev, 2),
            "avg_lead_time_days": round(avg_lead_time_prev, 1),
            "on_time_delivery_rate_%": round(on_time_prev, 1),
        },
        "variance": variance,
        "breakdowns": {
            "csat_by_region": csat_by_region,
            "csat_by_channel": csat_by_channel,
            "csat_by_segment": csat_by_segment,
            "csat_by_category": csat_by_category,
        },
        "marketing_effectiveness": {"top_campaign_channels": top_campaigns},
        "trends": trends,
    }

    # === AI insight + forecast ===
    cache_key = generate_ai_cache_key(cx_payload, start_dt, end_dt, "customer_experience")
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(
            cx_payload,
            str(start_dt.date()),
            str(end_dt.date()),
            "customer_experience_analytics",
            cache_key,
            "cx_metrics",
        ),
        daemon=True,
    ).start()

    return Response({**cx_payload, "data_key": cache_key, "ai_status": "processing"})

