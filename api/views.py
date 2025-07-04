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
from rest_framework import status
from dateutil.relativedelta import relativedelta
from django.utils.dateparse import parse_date
from django.http import JsonResponse
import sys
from django.core.cache import cache
import hashlib
import json

def python_version_view(request):
    return JsonResponse({"python_version": sys.version})
EXCEL_PATH = os.path.join(os.path.dirname(__file__), 'data/data.xlsx')
SHEET_NAME = 'salesData'


def load_data():
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    df['Created Date'] = pd.to_datetime(df['Created Date'])
    return df

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

# @api_view(["GET"])
# def sales_analytics(request):
#     period = request.GET.get("period", "monthly")
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         df_all = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#     # Clean currency columns
#     df_all["Net Extended Line Cost"] = df_all["Net Extended Line Cost"].apply(parse_float)
#     df_all["Cost Price"] = df_all["Cost Price"].apply(parse_float)

#     df_all["Created Date"] = pd.to_datetime(df_all["Created Date"])

#     # Parse start and end dates
#     try:
#         start_date_dt = pd.to_datetime(start_date) if start_date else df_all["Created Date"].min()
#         end_date_dt = pd.to_datetime(end_date) if end_date else df_all["Created Date"].max()
#     except:
#         return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)

#     # Filter current period
#     df_current = df_all[
#         (df_all["Created Date"] >= start_date_dt) &
#         (df_all["Created Date"] <= end_date_dt)
#     ].copy()

#     # Apply period granularity
#     if period == "daily":
#         df_current["Period"] = df_current["Created Date"].dt.date
#         delta = timedelta(days=1)
#     elif period == "weekly":
#         df_current["Period"] = df_current["Created Date"].dt.to_period("W").apply(lambda r: r.start_time)
#         delta = timedelta(weeks=1)
#     elif period == "monthly":
#         df_current["Period"] = df_current["Created Date"].dt.to_period("M").apply(lambda r: r.start_time)
#         delta = relativedelta(months=1)
#     elif period == "yearly":
#         df_current["Period"] = df_current["Created Date"].dt.to_period("Y").apply(lambda r: r.start_time)
#         delta = relativedelta(years=1)
#     else:
#         return Response({"error": "Invalid period. Use 'daily', 'weekly', 'monthly', or 'yearly'."}, status=status.HTTP_400_BAD_REQUEST)

#     # ----------------------------
#     # Current Period Metrics
#     # ----------------------------
#     total_sales_value = df_current["Net Extended Line Cost"].sum()
#     total_orders = df_current["Order Number"].nunique()
#     avg_order_value = total_sales_value / total_orders if total_orders else 0

#     # ----------------------------
#     # Compute Previous Period
#     # ----------------------------
#     range_length = end_date_dt - start_date_dt
#     previous_start = start_date_dt - range_length
#     previous_end = start_date_dt

#     df_previous = df_all[
#         (df_all["Created Date"] >= previous_start) &
#         (df_all["Created Date"] < previous_end)
#     ]

#     sales_previous = df_previous["Net Extended Line Cost"].sum()
#     sales_current = total_sales_value

#     if sales_previous == 0:
#         sales_growth = 100.0 if sales_current > 0 else 0
#     else:
#         sales_growth = ((sales_current - sales_previous) / abs(sales_previous)) * 100

#     # ----------------------------
#     # Performance Breakdown
#     # ----------------------------
#     performance_breakdown = (
#         df_current.groupby("Period")["Net Extended Line Cost"]
#         .sum()
#         .sort_index()
#         .reset_index()
#         .rename(columns={"Net Extended Line Cost": "sales"})
#     )
#     performance_breakdown["sales"] = performance_breakdown["sales"].round(2)

#     # ----------------------------
#     # Top Products
#     # ----------------------------
#     top_products = (
#         df_current.groupby("Product Description")["Net Extended Line Cost"]
#         .sum()
#         .sort_values(ascending=False)
#         .head(5)
#         .reset_index()
#         .rename(columns={"Net Extended Line Cost": "sales"})
#     )
#     top_products["sales"] = top_products["sales"].round(2)

#     # ----------------------------
#     # Customer Value
#     # ----------------------------
#     customer_value = (
#         df_current.groupby("Sender Name")["Net Extended Line Cost"]
#         .sum()
#         .sort_values(ascending=False)
#         .reset_index()
#         .rename(columns={"Net Extended Line Cost": "value"})
#     )
#     customer_value["value"] = customer_value["value"].round(2)

#     # ----------------------------
#     # Insight & Forecast
#     # ----------------------------
#     summary_payload = {
#         "total_sales_value": round(total_sales_value, 2),
#         "total_orders": total_orders,
#         "avg_order_value": round(avg_order_value, 2),
#         "sales_growth_percent": round(sales_growth, 2),
#         "sales_performance_breakdown": performance_breakdown.to_dict(orient="records"),
#         "top_products": top_products.to_dict(orient="records"),
#         "customer_value": customer_value.to_dict(orient="records"),
#     }

#     try:
#         insight_text, forecast_text = get_insight_and_forecast(summary_payload, str(start_date_dt.date()), str(end_date_dt.date()), period)
#     except Exception as e:
#         insight_text = f"Insight generation failed: {str(e)}"
#         forecast_text = f"Forecast generation failed: {str(e)}"

#     return Response({
#         **summary_payload,
#         "insight": insight_text,
#         "forecast": forecast_text
#     })

@api_view(["GET"])
def sales_analytics(request):
    period = request.GET.get("period", "monthly")
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df_all = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Clean and prepare data
    df_all["Net Extended Line Cost"] = df_all["Net Extended Line Cost"].apply(parse_float)
    df_all["Cost Price"] = df_all["Cost Price"].apply(parse_float)
    df_all["Created Date"] = pd.to_datetime(df_all["Created Date"])

    try:
        start_date_dt = pd.to_datetime(start_date) if start_date else df_all["Created Date"].min()
        end_date_dt = pd.to_datetime(end_date) if end_date else df_all["Created Date"].max()
    except:
        return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)

    df_current = df_all[(df_all["Created Date"] >= start_date_dt) & (df_all["Created Date"] <= end_date_dt)].copy()

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

    total_sales_value = df_current["Net Extended Line Cost"].sum()
    total_orders = df_current["Order Number"].nunique()
    avg_order_value = total_sales_value / total_orders if total_orders else 0

    range_length = end_date_dt - start_date_dt
    previous_start = start_date_dt - range_length
    previous_end = start_date_dt

    df_previous = df_all[(df_all["Created Date"] >= previous_start) & (df_all["Created Date"] < previous_end)]
    sales_previous = df_previous["Net Extended Line Cost"].sum()
    sales_growth = ((total_sales_value - sales_previous) / abs(sales_previous) * 100) if sales_previous else (100.0 if total_sales_value > 0 else 0)

    performance_breakdown = (
        df_current.groupby("Period")["Net Extended Line Cost"]
        .sum().sort_index().reset_index()
        .rename(columns={"Net Extended Line Cost": "sales"})
    )
    performance_breakdown["sales"] = performance_breakdown["sales"].round(2)

    top_products = (
        df_current.groupby("Product Description")["Net Extended Line Cost"]
        .sum().sort_values(ascending=False).head(5).reset_index()
        .rename(columns={"Net Extended Line Cost": "sales"})
    )
    top_products["sales"] = top_products["sales"].round(2)

    customer_value = (
        df_current.groupby("Sender Name")["Net Extended Line Cost"]
        .sum().sort_values(ascending=False).reset_index()
        .rename(columns={"Net Extended Line Cost": "value"})
    )
    customer_value["value"] = customer_value["value"].round(2)

    summary_payload = {
        "total_sales_value": round(total_sales_value, 2),
        "total_orders": total_orders,
        "avg_order_value": round(avg_order_value, 2),
        "sales_growth_percent": round(sales_growth, 2),
        "sales_performance_breakdown": performance_breakdown.to_dict(orient="records"),
        "top_products": top_products.to_dict(orient="records"),
        "customer_value": customer_value.to_dict(orient="records"),
    }

    cache_key = generate_ai_cache_key(summary_payload, start_date_dt, end_date_dt, period)

    # Set cache status and run AI generation in background
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    # threading.Thread(target=generate_insight_and_forecast_background, args=(summary_payload, start_date_dt, end_date_dt, period, cache_key)).start()
    threading.Thread(
    target=generate_insight_and_forecast_background,args=(summary_payload, start_date_dt, end_date_dt, period, cache_key, "sales_analytics")).start()
    
    return Response({
        **summary_payload,
        "ai_status": "processing",
        "data_key": cache_key
    })

# def generate_insight_and_forecast_background(summary_payload, start_date_dt, end_date_dt, period, cache_key):
#     try:
#         insight, forecast = get_insight_and_forecast(summary_payload, str(start_date_dt.date()), str(end_date_dt.date()), period)
#         cache.set(cache_key + ":insight", insight, timeout=60)
#         cache.set(cache_key + ":forecast", forecast, timeout=60)
#         cache.set(cache_key + ":status", {"insight": "completed", "forecast": "completed", "timestamp": datetime.now().isoformat()}, timeout=60)
#     except Exception as e:
#         cache.set(cache_key + ":insight", f"Insight generation failed: {str(e)}", timeout=60)
#         cache.set(cache_key + ":forecast", f"Forecast generation failed: {str(e)}", timeout=60)
#         cache.set(cache_key + ":status", {"insight": "failed", "forecast": "failed", "error": str(e)}, timeout=60)


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


# @api_view(["GET"])
# def sales_trend_analytics(request):
#     period = request.GET.get("period", "monthly")  # Optional: 'weekly', 'monthly', 'yearly'
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     df["Created Date"] = pd.to_datetime(df["Created Date"])
#     df["Net Extended Line Cost"] = df["Net Extended Line Cost"].apply(parse_float)

#     today = df["Created Date"].max().normalize()

#     # Determine custom or default period
#     if start_date and end_date:
#         try:
#             start_current = pd.to_datetime(start_date)
#             end_current = pd.to_datetime(end_date)
#         except Exception:
#             return Response({"error": "Invalid start_date or end_date format. Use YYYY-MM-DD."}, status=400)

#         duration = end_current - start_current
#         start_previous = start_current - duration - timedelta(days=1)
#         end_previous = start_current - timedelta(days=1)

#         # Dynamic frequency
#         days = duration.days
#         if days <= 7:
#             freq = "D"
#             label_format = "%Y-%m-%d"
#         elif days <= 31:
#             freq = "W-MON"
#             label_format = "Week %W"
#         elif days <= 365:
#             freq = "M"
#             label_format = "%B"
#         else:
#             freq = "Q"
#             label_format = "Q%q %Y"
#     else:
#         # fallback to predefined period
#         if period == "weekly":
#             start_current = today - timedelta(days=today.weekday())
#             end_current = start_current + timedelta(days=6)
#             start_previous = start_current - timedelta(weeks=1)
#             end_previous = start_current - timedelta(days=1)
#             freq = "D"
#             label_format = "%Y-%m-%d"
#         elif period == "monthly":
#             start_current = today.replace(day=1)
#             end_current = (start_current + relativedelta(months=1)) - timedelta(days=1)
#             start_previous = start_current - relativedelta(months=1)
#             end_previous = start_current - timedelta(days=1)
#             freq = "W-MON"
#             label_format = "Week %W"
#         elif period == "yearly":
#             start_current = today.replace(month=1, day=1)
#             end_current = today.replace(month=12, day=31)
#             start_previous = start_current - relativedelta(years=1)
#             end_previous = start_current - timedelta(days=1)
#             freq = "M"
#             label_format = "%B"
#         else:
#             return Response({"error": "Missing or invalid period. Provide either a period or start_date and end_date."}, status=400)

#     # Filter periods
#     df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)]
#     df_previous = df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)]

#     # Handle missing data
#     if df_current.empty:
#         return Response({"error": "No sales records found for the current period."}, status=404)

#     if df_previous.empty:
#         return Response({"error": "No sales records found for the previous period."}, status=404)

#     # Totals
#     total_sales_current = df_current["Net Extended Line Cost"].sum()
#     total_sales_previous = df_previous["Net Extended Line Cost"].sum()

#     growth_percent = (
#         ((total_sales_current - total_sales_previous) / total_sales_previous) * 100
#         if total_sales_previous != 0 else (100.0 if total_sales_current else 0.0)
#     )

#     def breakdown(df_slice, freq, label_format):
#         df_slice = df_slice.copy()
#         df_slice["Group"] = df_slice["Created Date"].dt.to_period(freq).dt.start_time
#         summary = df_slice.groupby("Group")["Net Extended Line Cost"].sum().reset_index()
#         summary.columns = ["period", "sales"]
#         summary["label"] = summary["period"].dt.strftime(label_format)
#         return summary.sort_values("period")

#     current_breakdown = breakdown(df_current, freq, label_format)
#     previous_breakdown = breakdown(df_previous, freq, label_format)

#     # Compute percentage growth trend
#     growth_trend_df = current_breakdown.copy()
#     growth_trend_df["growth_percent"] = growth_trend_df["sales"].pct_change() * 100
#     growth_trend_df["growth_percent"] = growth_trend_df["growth_percent"].round(2)
#     growth_trend_df["growth_percent"] = growth_trend_df["growth_percent"].fillna(0.0)

#     # Sub-period breakdowns
#     sub_freq, sub_label_format = get_sub_frequency(freq)
#     detailed_current_breakdown = breakdown(df_current, sub_freq, sub_label_format)
#     detailed_previous_breakdown = breakdown(df_previous, sub_freq, sub_label_format)

#     # Best-performing time
#     best_time = current_breakdown.sort_values("sales", ascending=False).iloc[0].to_dict() if not current_breakdown.empty else {}

#     # Product-level breakdown
#     product_sales = (
#         df_current.groupby("Product Description")["Net Extended Line Cost"]
#         .sum()
#         .sort_values(ascending=False)
#         .reset_index()
#         .rename(columns={"Net Extended Line Cost": "sales"})
#     )

#     # Quarterly breakdown for long periods
#     quarterly_breakdown = []
#     if (period == "yearly") or (start_date and end_date and duration.days > 180):
#         df_current["Quarter"] = df_current["Created Date"].dt.to_period("Q").dt.start_time
#         quarterly_breakdown = (
#             df_current.groupby("Quarter")["Net Extended Line Cost"]
#             .sum()
#             .reset_index()
#             .rename(columns={"Net Extended Line Cost": "sales", "Quarter": "quarter"})
#         )
#         quarterly_breakdown["quarter"] = quarterly_breakdown["quarter"].dt.strftime("Q%q %Y")

#     return Response({
#     "period": period or "custom",
#     "start_current": start_current.date(),
#     "end_current": end_current.date(),
#     "start_previous": start_previous.date(),
#     "end_previous": end_previous.date(),
#     "total_sales_current": round(total_sales_current, 2),
#     "total_sales_previous": round(total_sales_previous, 2),
#     "period_growth_percent": round(growth_percent, 2),
#     "best_time_period": {
#         "period": best_time.get("period"),
#         "sales": round(best_time.get("sales", 0), 2),
#         "label": best_time.get("label")
#     } if best_time else {},
#     "current_period_breakdown": current_breakdown.round(2).to_dict(orient="records"),
#     "previous_period_breakdown": previous_breakdown.round(2).to_dict(orient="records"),
#     "detailed_current_period_breakdown": detailed_current_breakdown.round(2).to_dict(orient="records"),
#     "detailed_previous_period_breakdown": detailed_previous_breakdown.round(2).to_dict(orient="records"),
#     "quarterly_breakdown": quarterly_breakdown.round(2).to_dict(orient="records") if not isinstance(quarterly_breakdown, list) else quarterly_breakdown,
#     "product_sales_breakdown": product_sales.round(2).to_dict(orient="records"),
#     "growth_trend": growth_trend_df[["label", "sales", "growth_percent"]].round(2).to_dict(orient="records"),
#     })

@api_view(["GET"])
def sales_trend_analytics(request):
    period = request.GET.get("period", "monthly")
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df["Created Date"] = pd.to_datetime(df["Created Date"])
    df["Net Extended Line Cost"] = df["Net Extended Line Cost"].apply(parse_float)

    today = df["Created Date"].max().normalize()

    # Determine time range
    if start_date and end_date:
        try:
            start_current = pd.to_datetime(start_date)
            end_current = pd.to_datetime(end_date)
        except Exception:
            return Response({"error": "Invalid start_date or end_date format. Use YYYY-MM-DD."}, status=400)

        duration = end_current - start_current
        start_previous = start_current - duration - timedelta(days=1)
        end_previous = start_current - timedelta(days=1)

        days = duration.days
        if days <= 7:
            freq, label_format = "D", "%Y-%m-%d"
        elif days <= 31:
            freq, label_format = "W-MON", "Week %W"
        elif days <= 365:
            freq, label_format = "M", "%B"
        else:
            freq, label_format = "Q", "Q%q %Y"
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
            return Response({"error": "Missing or invalid period. Provide either a period or start_date and end_date."}, status=400)

    df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)]
    df_previous = df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)]

    if df_current.empty:
        return Response({"error": "No sales records found for the current period."}, status=404)
    if df_previous.empty:
        return Response({"error": "No sales records found for the previous period."}, status=404)

    total_sales_current = df_current["Net Extended Line Cost"].sum()
    total_sales_previous = df_previous["Net Extended Line Cost"].sum()

    growth_percent = (
        ((total_sales_current - total_sales_previous) / total_sales_previous) * 100
        if total_sales_previous else (100.0 if total_sales_current else 0.0)
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

    growth_trend_df = current_breakdown.copy()
    growth_trend_df["growth_percent"] = growth_trend_df["sales"].pct_change() * 100
    growth_trend_df["growth_percent"] = growth_trend_df["growth_percent"].round(2).fillna(0.0)

    sub_freq, sub_label_format = get_sub_frequency(freq)
    detailed_current_breakdown = breakdown(df_current, sub_freq, sub_label_format)
    detailed_previous_breakdown = breakdown(df_previous, sub_freq, sub_label_format)

    best_time = current_breakdown.sort_values("sales", ascending=False).iloc[0].to_dict() if not current_breakdown.empty else {}

    product_sales = (
        df_current.groupby("Product Description")["Net Extended Line Cost"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"Net Extended Line Cost": "sales"})
    )

    quarterly_breakdown = []
    if (period == "yearly") or (start_date and end_date and (end_current - start_current).days > 180):
        df_current["Quarter"] = df_current["Created Date"].dt.to_period("Q").dt.start_time
        quarterly_breakdown = (
            df_current.groupby("Quarter")["Net Extended Line Cost"]
            .sum()
            .reset_index()
            .rename(columns={"Net Extended Line Cost": "sales", "Quarter": "quarter"})
        )
        quarterly_breakdown["quarter"] = quarterly_breakdown["quarter"].dt.strftime("Q%q %Y")

    # AI Summary Payload
    summary_payload = {
        "period": period or "custom",
        "start_current": str(start_current.date()),
        "end_current": str(end_current.date()),
        "total_sales_current": round(total_sales_current, 2),
        "total_sales_previous": round(total_sales_previous, 2),
        "period_growth_percent": round(growth_percent, 2),
        "product_sales_breakdown": product_sales.round(2).to_dict(orient="records"),
        "growth_trend": growth_trend_df[["label", "sales", "growth_percent"]].round(2).to_dict(orient="records"),
        "best_time_period": {
            "period": best_time.get("period"),
            "sales": round(best_time.get("sales", 0), 2),
            "label": best_time.get("label")
        } if best_time else {},
    }

    # AI Cache Key & Background Thread
    cache_key = generate_ai_cache_key(summary_payload, start_current, end_current, period)
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)
    threading.Thread(target=generate_insight_and_forecast_background, args=(
        summary_payload, start_current, end_current, period, cache_key, "sales_trend_analytics"
    )).start()

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
        "ai_status": "processing",
        "data_key": cache_key
    })


# @api_view(["GET"])
# def profit_margin_analytics(request):
#     period = request.GET.get("period", "monthly")
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     df["Created Date"] = pd.to_datetime(df["Created Date"])
#     df["Cost Price"] = df["Cost Price"].replace(",", "", regex=True).astype(float)
#     df["Net Extended Line Cost"] = df["Net Extended Line Cost"].replace(",", "", regex=True).astype(float)
#     df["Requested Qty"] = df["Requested Qty"].astype(float)

#     # Calculate Profit & Margin
#     df["Profit"] = df["Net Extended Line Cost"] - (df["Cost Price"] * df["Requested Qty"])
#     df["Profit Margin"] = df["Profit"] / df["Net Extended Line Cost"].replace(0, pd.NA) * 100

#     today = df["Created Date"].max().normalize()

#     # Determine current and previous periods
#     if start_date and end_date:
#         start_current = pd.to_datetime(start_date)
#         end_current = pd.to_datetime(end_date)
#         duration = end_current - start_current
#         start_previous = start_current - duration - timedelta(days=1)
#         end_previous = start_current - timedelta(days=1)

#         days = duration.days
#         if days <= 7:
#             freq = "D"
#             label_format = "%Y-%m-%d"
#         elif days <= 31:
#             freq = "W-MON"
#             label_format = "Week %W"
#         elif days <= 365:
#             freq = "M"
#             label_format = "%B"
#         else:
#             freq = "Q"
#             label_format = "Q%q %Y"
#     else:
#         if period == "weekly":
#             start_current = today - timedelta(days=today.weekday())
#             end_current = start_current + timedelta(days=6)
#             start_previous = start_current - timedelta(weeks=1)
#             end_previous = start_current - timedelta(days=1)
#             freq = "D"
#             label_format = "%Y-%m-%d"
#         elif period == "monthly":
#             start_current = today.replace(day=1)
#             end_current = (start_current + relativedelta(months=1)) - timedelta(days=1)
#             start_previous = start_current - relativedelta(months=1)
#             end_previous = start_current - timedelta(days=1)
#             freq = "W-MON"
#             label_format = "Week %W"
#         elif period == "yearly":
#             start_current = today.replace(month=1, day=1)
#             end_current = today.replace(month=12, day=31)
#             start_previous = start_current - relativedelta(years=1)
#             end_previous = start_current - timedelta(days=1)
#             freq = "M"
#             label_format = "%B"
#         else:
#             return Response({"error": "Missing or invalid period. Provide either a valid 'period' or both 'start_date' and 'end_date'."}, status=400)

#     # Slice data
#     df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)]
#     df_previous = df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)]

#     if df_current.empty:
#         return Response({"error": "No profit data found for the current period."}, status=404)
#     if df_previous.empty:
#         return Response({"error": "No profit data found for the previous period."}, status=404)

#     # Aggregates
#     profit_current = df_current["Profit"].sum()
#     revenue_current = df_current["Net Extended Line Cost"].sum()
#     profit_margin_current = (profit_current / revenue_current * 100) if revenue_current else 0

#     profit_previous = df_previous["Profit"].sum()
#     revenue_previous = df_previous["Net Extended Line Cost"].sum()
#     profit_margin_previous = (profit_previous / revenue_previous * 100) if revenue_previous else 0

#     profit_growth = ((profit_current - profit_previous) / profit_previous * 100) if profit_previous else (100 if profit_current else 0)

#     def breakdown(df_slice):
#         df_slice = df_slice.copy()
#         df_slice["Period"] = df_slice["Created Date"].dt.to_period(freq).dt.start_time
#         summary = df_slice.groupby("Period").agg(
#             revenue=("Net Extended Line Cost", "sum"),
#             cost=("Cost Price", lambda x: (x * df_slice.loc[x.index, "Requested Qty"]).sum()),
#             profit=("Profit", "sum"),
#         ).reset_index()
#         summary["label"] = summary["Period"].dt.strftime(label_format)
#         summary["profit_margin"] = summary.apply(
#             lambda row: (row["profit"] / row["revenue"] * 100) if row["revenue"] else 0, axis=1
#         )
#         return summary.round(2).sort_values("Period")  # <-- round all float columns to 2dp

#     current_breakdown = breakdown(df_current)
#     previous_breakdown = breakdown(df_previous)

#     return Response({
#         "period": period or "custom",
#         "start_current": start_current.date(),
#         "end_current": end_current.date(),
#         "start_previous": start_previous.date(),
#         "end_previous": end_previous.date(),
#         "total_profit_current": round(profit_current, 2),
#         "total_profit_previous": round(profit_previous, 2),
#         "profit_growth_percent": round(profit_growth, 2),
#         "profit_margin_current": round(profit_margin_current, 2),
#         "profit_margin_previous": round(profit_margin_previous, 2),
#         "current_period_breakdown": current_breakdown.to_dict(orient="records"),
#         "previous_period_breakdown": previous_breakdown.to_dict(orient="records"),
#     })

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

    df["Profit"] = df["Net Extended Line Cost"] - (df["Cost Price"] * df["Requested Qty"])
    df["Profit Margin"] = df["Profit"] / df["Net Extended Line Cost"].replace(0, pd.NA) * 100

    today = df["Created Date"].max().normalize()

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

    df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)]
    df_previous = df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)]

    if df_current.empty:
        return Response({"error": "No profit data found for the current period."}, status=404)
    if df_previous.empty:
        return Response({"error": "No profit data found for the previous period."}, status=404)

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
        return summary.round(2).sort_values("Period")

    current_breakdown = breakdown(df_current)
    previous_breakdown = breakdown(df_previous)

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

# @api_view(['GET'])
# def cost_analysis(request):
#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     # Clean columns
#     df["Net Extended Line Cost"] = df["Net Extended Line Cost"].apply(parse_float)
#     df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')

#     # Query params
#     period = request.query_params.get("period")
#     store_filter = request.query_params.get("store")
#     product_filter = request.query_params.get("product")
#     start_date_param = request.query_params.get("start_date")
#     end_date_param = request.query_params.get("end_date")

#     today = pd.Timestamp.today().normalize()
#     trend_freq = "W"

#     try:
#         if start_date_param and end_date_param:
#             start_current = pd.to_datetime(start_date_param)
#             end_current = pd.to_datetime(end_date_param)

#             if start_current > end_current:
#                 return Response({"error": "start_date cannot be after end_date."}, status=400)

#             delta_days = (end_current - start_current).days
#             if delta_days <= 14:
#                 trend_freq = "D"
#             elif delta_days <= 60:
#                 trend_freq = "W"
#             else:
#                 trend_freq = "M"

#             start_previous = end_previous = None

#         elif period == "week":
#             start_current = today - pd.to_timedelta(today.weekday(), unit='d')
#             end_current = start_current + pd.Timedelta(days=6)
#             start_previous = start_current - pd.Timedelta(days=7)
#             end_previous = start_previous + pd.Timedelta(days=6)
#             trend_freq = "D"

#         elif period == "month":
#             start_current = today.replace(day=1)
#             end_current = start_current + pd.offsets.MonthEnd(1)
#             start_previous = (start_current - pd.offsets.MonthBegin(1)).replace(day=1)
#             end_previous = start_previous + pd.offsets.MonthEnd(1)
#             trend_freq = "W"

#         elif period == "year":
#             start_current = today.replace(month=1, day=1)
#             end_current = start_current + pd.offsets.YearEnd(1)
#             start_previous = (start_current - pd.offsets.YearBegin(1)).replace(month=1, day=1)
#             end_previous = start_previous + pd.offsets.YearEnd(1)
#             trend_freq = "M"

#         else:
#             return Response({"error": "Provide valid 'period' or 'start_date' and 'end_date'."}, status=400)

#     except Exception as e:
#         return Response({"error": f"Invalid date input: {str(e)}"}, status=400)

#     # Date bounds check
#     min_date = df["Created Date"].min()
#     max_date = df["Created Date"].max()
#     if start_current > max_date or end_current < min_date:
#         return Response({
#             "error": "Provided date range is outside the available data range.",
#             "data_available_from": str(min_date.date()),
#             "data_available_to": str(max_date.date())
#         }, status=404)

#     # Filter current and previous periods
#     df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)].copy()
#     df_previous = (
#         df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)].copy()
#         if start_previous and end_previous else pd.DataFrame(columns=df.columns)
#     )

#     if df_current.empty:
#         return Response({"error": "No data available for the current period."}, status=404)

#     # Apply filters
#     if store_filter:
#         df_current = df_current[df_current["Store Name"].str.lower() == store_filter.lower()]
#     if product_filter:
#         df_current = df_current[df_current["Product Description"].str.lower() == product_filter.lower()]

#     # Proceed with analysis
#     total_cost_current = df_current["Net Extended Line Cost"].sum()
#     total_cost_previous = df_previous["Net Extended Line Cost"].sum()
#     growth_percent = (
#         ((total_cost_current - total_cost_previous) / total_cost_previous) * 100
#         if total_cost_previous else 0
#     )

#     trend_current = df_current.set_index("Created Date")["Net Extended Line Cost"].resample(trend_freq).sum().reset_index()
#     trend_current["Net Extended Line Cost"] = trend_current["Net Extended Line Cost"].round(2)

#     trend_previous = (
#         df_previous.set_index("Created Date")["Net Extended Line Cost"].resample(trend_freq).sum().reset_index()
#         if not df_previous.empty else []
#     )
#     if not isinstance(trend_previous, list):
#         trend_previous["Net Extended Line Cost"] = trend_previous["Net Extended Line Cost"].round(2)

#     # Breakdown & summaries
#     product_costs = (
#         df_current.groupby("Product Description")["Net Extended Line Cost"]
#         .sum().reset_index().rename(columns={"Net Extended Line Cost": "Total Cost"})
#         .sort_values("Total Cost", ascending=False)
#     )
#     product_costs["Total Cost"] = product_costs["Total Cost"].round(2)

#     store_costs = (
#         df_current.groupby("Store Name")["Net Extended Line Cost"]
#         .sum().reset_index().rename(columns={"Net Extended Line Cost": "Total Cost"})
#         .sort_values("Total Cost", ascending=False)
#     )
#     store_costs["Total Cost"] = store_costs["Total Cost"].round(2)

#     product_count_per_store = (
#         df_current.groupby("Store Name")["Product Description"]
#         .nunique().reset_index().rename(columns={"Product Description": "Unique Product Count"})
#     )

#     # Safely round only numeric fields
#     if not product_costs.empty:
#         row = product_costs.iloc[0]
#         most_expensive_product = {
#             "Product Description": row["Product Description"],
#             "Total Cost": round(row["Total Cost"], 2)
#         }
#     else:
#         most_expensive_product = {}

#     if not store_costs.empty:
#         row = store_costs.iloc[0]
#         most_expensive_store = {
#             "Store Name": row["Store Name"],
#             "Total Cost": round(row["Total Cost"], 2)
#         }
#     else:
#         most_expensive_store = {}

#     return Response({
#         "mode": "custom" if start_date_param else period,
#         "start_current": start_current.date(),
#         "end_current": end_current.date(),
#         "start_previous": start_previous.date() if start_previous else None,
#         "end_previous": end_previous.date() if end_previous else None,
#         "total_cost_current": round(total_cost_current, 2),
#         "total_cost_previous": round(total_cost_previous, 2) if not df_previous.empty else None,
#         "cost_growth_percent": round(growth_percent, 2) if not df_previous.empty else None,
#         "current_period_cost_trend": trend_current.to_dict(orient="records"),
#         "previous_period_cost_trend": trend_previous if isinstance(trend_previous, list) else trend_previous.to_dict(orient="records"),
#         "product_cost_breakdown": product_costs.to_dict(orient="records"),
#         "store_cost_breakdown": store_costs.to_dict(orient="records"),
#         "products_involved": sorted(df_current["Product Description"].dropna().unique().tolist()),
#         "stores_involved": sorted(df_current["Store Name"].dropna().unique().tolist()),
#         "unique_products_per_store": product_count_per_store.to_dict(orient="records"),
#         "most_expensive_product": most_expensive_product,
#         "most_expensive_store": most_expensive_store,
#         "filters_applied": {
#             "store": store_filter,
#             "product": product_filter,
#             "start_date": start_date_param,
#             "end_date": end_date_param
#         }
#     })


@api_view(['GET'])
def cost_analysis(request):
    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

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
            trend_freq = "D" if delta_days <= 14 else "W" if delta_days <= 60 else "M"
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
            end_current = today + pd.offsets.YearEnd(0)
            start_previous = (start_current - pd.offsets.YearBegin(1)).replace(month=1, day=1)
            end_previous = start_previous + pd.offsets.YearEnd(1)
            trend_freq = "M"

        else:
            return Response({"error": "Provide valid 'period' or 'start_date' and 'end_date'."}, status=400)

    except Exception as e:
        return Response({"error": f"Invalid date input: {str(e)}"}, status=400)

    min_date = df["Created Date"].min()
    max_date = df["Created Date"].max()
    if start_current > max_date or end_current < min_date:
        return Response({
            "error": "Provided date range is outside the available data range.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    df_current = df[(df["Created Date"] >= start_current) & (df["Created Date"] <= end_current)].copy()
    df_previous = (
        df[(df["Created Date"] >= start_previous) & (df["Created Date"] <= end_previous)].copy()
        if start_previous and end_previous else pd.DataFrame(columns=df.columns)
    )

    if df_current.empty:
        return Response({"error": "No data available for the current period."}, status=404)

    if store_filter:
        df_current = df_current[df_current["Store Name"].str.lower() == store_filter.lower()]
    if product_filter:
        df_current = df_current[df_current["Product Description"].str.lower() == product_filter.lower()]

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

    most_expensive_product = product_costs.iloc[0].to_dict() if not product_costs.empty else {}
    most_expensive_store = store_costs.iloc[0].to_dict() if not store_costs.empty else {}

    # -------------------------
    # AI Insight/Forecast Setup
    # -------------------------
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
    # Set cache status and run AI generation in background
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
        },
        "data_key": cache_key
    })

# @api_view(['GET'])
# def sales_summary(request):
#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     # Clean and prepare DataFrame
#     df['Created Date'] = pd.to_datetime(df['Created Date'], errors='coerce')
#     df['Net Extended Line Cost'] = pd.to_numeric(df['Net Extended Line Cost'].astype(str).str.replace(',', ''), errors='coerce')
#     df['Requested Qty'] = pd.to_numeric(df['Requested Qty'], errors='coerce')

#     # Parse query parameters
#     start_date_str = request.GET.get('start_date')
#     end_date_str = request.GET.get('end_date')

#     try:
#         start_date = pd.to_datetime(start_date_str) if start_date_str else None
#         end_date = pd.to_datetime(end_date_str) if end_date_str else None
#         if start_date and end_date and start_date > end_date:
#             return Response({"error": "start_date cannot be after end_date."}, status=400)
#     except Exception as e:
#         return Response({"error": f"Invalid date format. Use YYYY-MM-DD. Details: {str(e)}"}, status=400)

#     # Validate that requested dates fall within dataset range
#     min_date = df['Created Date'].min()
#     max_date = df['Created Date'].max()
#     if (start_date and start_date > max_date) or (end_date and end_date < min_date):
#         return Response({
#             "error": "Provided date range is outside the available data range.",
#             "data_available_from": str(min_date.date()),
#             "data_available_to": str(max_date.date())
#         }, status=404)

#     # Filter by date
#     if start_date:
#         df = df[df['Created Date'] >= start_date]
#     if end_date:
#         df = df[df['Created Date'] <= end_date]

#     if df.empty:
#         return Response({"message": "No data available for the specified period."}, status=404)

#     # Summary calculations
#     total_orders = df['Order Number'].nunique()
#     new_orders = df[['Order Number', 'Created Date']].drop_duplicates().shape[0]
#     total_revenue = df['Net Extended Line Cost'].sum()
#     avg_sales = df['Net Extended Line Cost'].mean()

#     # Top products
#     top_products = (
#         df.groupby('Product Description')
#         .agg(
#             total_sales=('Net Extended Line Cost', 'sum'),
#             quantity_sold=('Requested Qty', 'sum')
#         )
#         .sort_values(by='total_sales', ascending=False)
#         .reset_index()
#         .head(5)
#     )

#     # Round float values in top products
#     top_products['total_sales'] = top_products['total_sales'].round(2)
#     top_products['quantity_sold'] = top_products['quantity_sold'].round(2)

#     return Response({
#         "summary": {
#             "start_date": start_date_str,
#             "end_date": end_date_str,
#             "total_orders": total_orders,
#             "new_orders": new_orders,
#             "total_revenue": round(total_revenue, 2),
#             "average_sales": round(avg_sales, 2)
#         },
#         "top_products": top_products.to_dict(orient='records')
#     })

@api_view(['GET'])
def sales_summary(request):
    try:
        df = load_data()
    except Exception as e:
        return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

    df['Created Date'] = pd.to_datetime(df['Created Date'], errors='coerce')
    df['Net Extended Line Cost'] = pd.to_numeric(df['Net Extended Line Cost'].astype(str).str.replace(',', ''), errors='coerce')
    df['Requested Qty'] = pd.to_numeric(df['Requested Qty'], errors='coerce')

    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')

    try:
        start_date = pd.to_datetime(start_date_str) if start_date_str else None
        end_date = pd.to_datetime(end_date_str) if end_date_str else None
        if start_date and end_date and start_date > end_date:
            return Response({"error": "start_date cannot be after end_date."}, status=400)
    except Exception as e:
        return Response({"error": f"Invalid date format. Use YYYY-MM-DD. Details: {str(e)}"}, status=400)

    min_date = df['Created Date'].min()
    max_date = df['Created Date'].max()
    if (start_date and start_date > max_date) or (end_date and end_date < min_date):
        return Response({
            "error": "Provided date range is outside the available data range.",
            "data_available_from": str(min_date.date()),
            "data_available_to": str(max_date.date())
        }, status=404)

    if start_date:
        df = df[df['Created Date'] >= start_date]
    if end_date:
        df = df[df['Created Date'] <= end_date]

    if df.empty:
        return Response({"message": "No data available for the specified period."}, status=404)

    total_orders = df['Order Number'].nunique()
    new_orders = df[['Order Number', 'Created Date']].drop_duplicates().shape[0]
    total_revenue = df['Net Extended Line Cost'].sum()
    avg_sales = df['Net Extended Line Cost'].mean()

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
    top_products['total_sales'] = top_products['total_sales'].round(2)
    top_products['quantity_sold'] = top_products['quantity_sold'].round(2)

    summary_payload = {
        "total_orders": total_orders,
        "new_orders": new_orders,
        "total_revenue": round(total_revenue, 2),
        "average_sales": round(avg_sales, 2),
        "top_products": top_products.to_dict(orient='records')
    }

    period = "custom"
    cache_key = generate_ai_cache_key(summary_payload, start_date or min_date, end_date or max_date, period)
    # Set cache status and run AI generation in background
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)
    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary_payload, str((start_date or min_date).date()), str((end_date or max_date).date()), period, cache_key, "sales_summary")
    ).start()

    return Response({
        "summary": {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "total_orders": total_orders,
            "new_orders": new_orders,
            "total_revenue": round(total_revenue, 2),
            "average_sales": round(avg_sales, 2)
        },
        "top_products": top_products.to_dict(orient='records'),
        "data_key": cache_key
    })


# @api_view(["GET"])
# def transaction_summary(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     if not start_date or not end_date:
#         return Response({"error": "start_date and end_date are required"}, status=400)

#     try:
#         start_date_parsed = pd.to_datetime(start_date)
#         end_date_parsed = pd.to_datetime(end_date)
#         if start_date_parsed > end_date_parsed:
#             return Response({"error": "start_date cannot be after end_date."}, status=400)
#     except Exception as e:
#         return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#     df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"].astype(str).str.replace(",", ""), errors="coerce")
#     df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce")

#     # Check available range
#     min_date = df["Created Date"].min()
#     max_date = df["Created Date"].max()
#     if (start_date_parsed > max_date) or (end_date_parsed < min_date):
#         return Response({
#             "error": "Provided date range is outside the available dataset.",
#             "data_available_from": str(min_date.date()),
#             "data_available_to": str(max_date.date())
#         }, status=404)

#     # Current period
#     current_df = df[(df["Created Date"] >= start_date_parsed) & (df["Created Date"] <= end_date_parsed)]
#     if current_df.empty:
#         return Response({"message": "No transactions found in the current period."}, status=404)

#     current_total_value = current_df['Net Extended Line Cost'].sum()
#     current_total_quantity = current_df['Requested Qty'].sum()
#     current_avg_order_value = current_df.groupby('Order Number')['Net Extended Line Cost'].sum().mean()

#     # Previous period
#     duration = end_date_parsed - start_date_parsed
#     prev_start = start_date_parsed - duration - timedelta(days=1)
#     prev_end = start_date_parsed - timedelta(days=1)

#     previous_df = df[(df["Created Date"] >= prev_start) & (df["Created Date"] <= prev_end)]
#     previous_total_value = previous_df['Net Extended Line Cost'].sum()
#     previous_total_quantity = previous_df['Requested Qty'].sum()
#     previous_avg_order_value = previous_df.groupby('Order Number')['Net Extended Line Cost'].sum().mean()

#     # Store-level summary (Top 20)
#     def store_summary(subset_df):
#         summary = subset_df.groupby('Store Name').agg({
#             'Requested Qty': 'sum',
#             'Net Extended Line Cost': 'sum'
#         }).round(2).reset_index()
#         summary['Requested Qty'] = summary['Requested Qty'].round(2)
#         summary['Net Extended Line Cost'] = summary['Net Extended Line Cost'].round(2)
#         return summary.sort_values('Net Extended Line Cost', ascending=False).head(20).to_dict(orient='records')

#     # Product-level summary (Top 20)
#     def product_summary(subset_df):
#         summary = subset_df.groupby('Product Description').agg({
#             'Requested Qty': 'sum',
#             'Net Extended Line Cost': 'sum'
#         }).round(2).reset_index()
#         summary['Requested Qty'] = summary['Requested Qty'].round(2)
#         summary['Net Extended Line Cost'] = summary['Net Extended Line Cost'].round(2)
#         return summary.sort_values('Net Extended Line Cost', ascending=False).head(20).to_dict(orient='records')

#     # Trend chart
#     def trend_chart(subset_df):
#         trend_df = subset_df.groupby(subset_df['Created Date'].dt.date)['Net Extended Line Cost'].sum().reset_index()
#         trend_df['Net Extended Line Cost'] = trend_df['Net Extended Line Cost'].round(2)
#         return trend_df.rename(columns={'Created Date': 'date', 'Net Extended Line Cost': 'revenue'}).to_dict(orient='records')

#     # Percentage change helper
#     def percentage_change(current, previous):
#         if previous == 0:
#             return None
#         return round(((current - previous) / previous) * 100, 2)

#     return Response({
#         "start_date": str(start_date_parsed.date()),
#         "end_date": str(end_date_parsed.date()),
#         "current_period": {
#             "total_transaction_value": round(current_total_value, 2),
#             "total_quantity": round(current_total_quantity, 2),
#             "average_order_value": round(current_avg_order_value or 0, 2),
#             "store_summary": store_summary(current_df),
#             "product_summary": product_summary(current_df),
#             "trend_chart": trend_chart(current_df),
#         },
#         "previous_period": {
#             "total_transaction_value": round(previous_total_value, 2),
#             "total_quantity": round(previous_total_quantity, 2),
#             "average_order_value": round(previous_avg_order_value or 0, 2),
#             "store_summary": store_summary(previous_df),
#             "product_summary": product_summary(previous_df),
#             "trend_chart": trend_chart(previous_df),
#         },
#         "percentage_changes": {
#             "transaction_value_change": percentage_change(current_total_value, previous_total_value),
#             "quantity_change": percentage_change(current_total_quantity, previous_total_quantity),
#             "average_order_value_change": percentage_change(current_avg_order_value, previous_avg_order_value),
#         }
#     })

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

    # Background insight/forecast
    cache_key = f"transaction_summary:{start_date}:{end_date}"
    # Set cache status and run AI generation in background
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)
    summary_data = {
        "current_total_value": round(current_total_value, 2),
        "current_total_quantity": round(current_total_quantity, 2),
        "current_avg_order_value": round(current_avg_order_value or 0, 2),
        "top_products": product_summary(current_df)[:5],
        "top_stores": store_summary(current_df)[:5]
    }
    threading.Thread(target=generate_insight_and_forecast_background, args=(summary_data, start_date, end_date, "custom", cache_key, "transaction_summary")).start()

    return Response({
        "start_date": str(start_date_parsed.date()),
        "end_date": str(end_date_parsed.date()),
        "ai_cache_key": cache_key,
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
        },
        "data_key": cache_key
    })

# @api_view(["GET"])
# def transaction_entities_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     store_filter = request.GET.get("store")
#     sender_filter = request.GET.get("sender")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
#     df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"].astype(str).str.replace(",", ""), errors="coerce")
#     df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce")
#     df = df.dropna(subset=["Created Date"])

#     try:
#         if start_date:
#             start = pd.to_datetime(start_date)
#             df = df[df["Created Date"] >= start]
#         if end_date:
#             end = pd.to_datetime(end_date)
#             df = df[df["Created Date"] <= end]
#     except Exception as e:
#         return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

#     if store_filter:
#         df = df[df["Store Name"] == store_filter]
#     if sender_filter:
#         df = df[df["Sender Name"] == sender_filter]

#     if df.empty:
#         return Response({"message": "No data found for selected filters"}, status=200)

#     total_revenue = df["Net Extended Line Cost"].sum()

#     try:
#         store_group = df.groupby("Store Name").agg(
#             revenue=("Net Extended Line Cost", "sum"),
#             orders=("Order Number", "nunique"),
#             quantity=("Requested Qty", "sum")
#         )
#         store_group["avg_order_value"] = store_group["revenue"] / store_group["orders"]
#         store_group["revenue_pct"] = (store_group["revenue"] / total_revenue * 100).round(2)
#         top_stores = store_group.sort_values("revenue", ascending=False).head(5).round(2).to_dict("index")

#         customer_group = df.groupby("Sender Name").agg(
#             revenue=("Net Extended Line Cost", "sum"),
#             orders=("Order Number", "nunique"),
#             quantity=("Requested Qty", "sum")
#         )
#         customer_group["avg_order_value"] = customer_group["revenue"] / customer_group["orders"]
#         customer_group["revenue_pct"] = (customer_group["revenue"] / total_revenue * 100).round(2)
#         top_customers_df = customer_group.sort_values("revenue", ascending=False).head(5).round(2)
#         top_customers = top_customers_df.to_dict("index")

#         df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
#         top_customer_names = list(top_customers_df.index)
#         customer_trend = (
#             df[df["Sender Name"].isin(top_customer_names)]
#             .groupby(["Sender Name", "Month"])["Net Extended Line Cost"]
#             .sum().reset_index()
#             .pivot(index="Month", columns="Sender Name", values="Net Extended Line Cost")
#             .fillna(0).round(2)
#         )
#         customer_trend_data = customer_trend.reset_index().to_dict(orient="records")

#         product_group = df.groupby("Product Description").agg(
#             revenue=("Net Extended Line Cost", "sum"),
#             quantity=("Requested Qty", "sum"),
#             orders=("Order Number", "nunique")
#         )
#         product_group["revenue_pct"] = (product_group["revenue"] / total_revenue * 100).round(2)
#         top_products = product_group.sort_values("revenue", ascending=False).head(5).round(2).to_dict("index")

#     except Exception as e:
#         return Response({"error": f"Failed during aggregation: {str(e)}"}, status=500)

#     return Response({
#         "filters_applied": {
#             "start_date": start_date,
#             "end_date": end_date,
#             "store_filter": store_filter,
#             "sender_filter": sender_filter
#         },
#         "top_stores": top_stores,
#         "top_customers": top_customers,
#         "top_products_by_revenue": top_products,
#         "monthly_customer_trend": customer_trend_data
#     })

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

        # AI Cache Key and Background Thread
        cache_key = f"transaction_entities_analysis:{start_date}:{end_date}:{store_filter}:{sender_filter}"
        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        summary_data = {
            "total_revenue": round(total_revenue, 2),
            "top_stores": top_stores,
            "top_customers": top_customers,
            "top_products": top_products
        }
        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=(summary_data, start_date, end_date, "custom", cache_key, "transaction_entities_analysis")
        ).start()

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
        "monthly_customer_trend": customer_trend_data,
        "data_key": cache_key
    })


# @api_view(["GET"])
# def transaction_timing_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#     df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors="coerce")
#     df = df.dropna(subset=["Created Date"])

#     try:
#         if start_date:
#             start = pd.to_datetime(start_date)
#             df = df[df["Created Date"] >= start]
#         if end_date:
#             end = pd.to_datetime(end_date)
#             df = df[df["Created Date"] <= end]
#     except Exception as e:
#         return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No transactions found for the selected period"}, status=200)

#     try:
#         df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
#         df["Week"] = df["Created Date"].dt.to_period("W").astype(str)
#         df["Day"] = df["Created Date"].dt.date.astype(str)
#         df["Weekday"] = df["Created Date"].dt.day_name()
#         df["Hour"] = df["Created Date"].dt.hour

#         freq_by_month = df.groupby("Month").size().to_dict()
#         freq_by_week = df.groupby("Week").size().to_dict()
#         freq_by_day = df.groupby("Day").size().to_dict()
#         freq_by_weekday = df.groupby("Weekday").size().sort_values(ascending=False).to_dict()
#         freq_by_hour = df.groupby("Hour").size().sort_index().to_dict()

#         df = df.dropna(subset=["Delivery Date"])
#         df["Fulfillment Days"] = (df["Delivery Date"] - df["Created Date"]).dt.days

#         avg_fulfillment = df["Fulfillment Days"].mean()
#         best_fulfillment = df["Fulfillment Days"].min()
#         worst_fulfillment = df["Fulfillment Days"].max()

#         df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
#         fulfillment_trend = df.groupby("Month")["Fulfillment Days"].mean().round(2).to_dict()

#     except Exception as e:
#         return Response({"error": f"Error during aggregation: {str(e)}"}, status=500)

#     return Response({
#         "frequency": {
#             "by_month": freq_by_month,
#             "by_week": freq_by_week,
#             "by_day": freq_by_day,
#             "by_weekday": freq_by_weekday,
#             "by_hour": freq_by_hour
#         },
#         "fulfillment": {
#             "average_days": round(avg_fulfillment, 2) if not pd.isna(avg_fulfillment) else None,
#             "fastest_days": int(best_fulfillment) if not pd.isna(best_fulfillment) else None,
#             "slowest_days": int(worst_fulfillment) if not pd.isna(worst_fulfillment) else None,
#             "monthly_trend": fulfillment_trend
#         }
#     })

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

    # --- AI Background Thread + Cache ---
    cache_key = f"transaction_timing_analysis:{start_date or 'null'}:{end_date or 'null'}"
    period = "month"  # can be inferred dynamically if needed

    # Set cache status
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
        args=(summary, start_date, end_date, period, cache_key, "transaction_timing_analysis")
    ).start()

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


# @api_view(["GET"])
# def product_demand_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
#     df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce")
#     df = df.dropna(subset=["Created Date", "Requested Qty"])

#     try:
#         if start_date:
#             start = pd.to_datetime(start_date)
#             df = df[df["Created Date"] >= start]
#         if end_date:
#             end = pd.to_datetime(end_date)
#             df = df[df["Created Date"] <= end]
#     except Exception as e:
#         return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No data found in the selected period."}, status=200)

#     try:
#         df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
#         df["Weekday"] = df["Created Date"].dt.day_name()

#         top_products = df.groupby("Product Description")["Requested Qty"].sum().sort_values(ascending=False).head(10)
#         trend = df.groupby(df["Created Date"].dt.date)["Requested Qty"].sum().reset_index()
#         trend.columns = ["date", "quantity"]

#         store_demand = (
#             df.groupby(["Store Name", "Product Description"])["Requested Qty"].sum()
#             .reset_index().sort_values(by="Requested Qty", ascending=False)
#         )

#         product_order_qty = df.groupby(["Product Description", "Order Number"])["Requested Qty"].sum().reset_index()
#         velocity = product_order_qty.groupby("Product Description")["Requested Qty"].mean().sort_values(ascending=False).head(10)

#         by_month = df.groupby("Month")["Requested Qty"].sum().to_dict()
#         by_weekday = df.groupby("Weekday")["Requested Qty"].sum().sort_values(ascending=False).to_dict()

#         parsed_start = pd.to_datetime(parse_date(start_date)) if start_date else df["Created Date"].min()
#         parsed_end = pd.to_datetime(parse_date(end_date)) if end_date else df["Created Date"].max()

#         if not parsed_start or not parsed_end:
#             raise ValueError("Invalid start or end date for comparison")
#         period_length = parsed_end - parsed_start

#         prev_start = parsed_start - period_length
#         prev_end = parsed_start - timedelta(days=1)
#         prev_df = df[(df["Created Date"] >= prev_start) & (df["Created Date"] <= prev_end)]

#         recent_top = df.groupby("Product Description")["Requested Qty"].sum()
#         prev_top = prev_df.groupby("Product Description")["Requested Qty"].sum()

#         combined_demand = pd.concat([recent_top, prev_top], axis=1, keys=["current", "previous"]).fillna(0)

#         combined_demand["pct_change"] = combined_demand.apply(safe_pct_change, axis=1)
#         rising_demand = combined_demand.sort_values("pct_change", ascending=False).head(5).round(2).to_dict(orient="index")

#         matrix = df.pivot_table(index="Store Name", columns="Product Description", values="Requested Qty", aggfunc="sum", fill_value=0)

#     except Exception as e:
#         return Response({"error": f"Failed to compute demand analysis: {str(e)}"}, status=500)

#     return Response({
#         "top_products_by_quantity": top_products.to_dict(),
#         "demand_trend_over_time": trend.to_dict(orient="records"),
#         "store_product_demand": store_demand.to_dict(orient="records"),
#         "demand_velocity_per_product": velocity.round(2).to_dict(),
#         "seasonality": {
#             "monthly_demand": {k: round(v, 2) for k, v in by_month.items()},
#             "weekday_demand": {k: round(v, 2) for k, v in by_weekday.items()}
#         },
#         "rising_product_demand": rising_demand,
#         "product_demand_matrix": matrix.astype(int).to_dict()
#     })


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
        combined_demand["pct_change"] = combined_demand.apply(safe_pct_change, axis=1)
        rising_demand = combined_demand.sort_values("pct_change", ascending=False).head(5).round(2).to_dict(orient="index")

        matrix = df.pivot_table(index="Store Name", columns="Product Description", values="Requested Qty", aggfunc="sum", fill_value=0)

    except Exception as e:
        return Response({"error": f"Failed to compute demand analysis: {str(e)}"}, status=500)

    # --- AI Insight + Forecast (background) ---
    cache_key = f"product_demand_analysis:{start_date or 'null'}:{end_date or 'null'}"
    period = "month"

    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    summary = {
        "top_products": top_products.head(5).round(2).to_dict(),
        "rising_demand": rising_demand,
        "velocity": velocity.head(5).round(2).to_dict(),
        "monthly_demand": by_month
    }

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary, start_date, end_date, period, cache_key, "product_demand_analysis")
    ).start()

    return Response({
        "top_products_by_quantity": top_products.to_dict(),
        "demand_trend_over_time": trend.to_dict(orient="records"),
        "store_product_demand": store_demand.to_dict(orient="records"),
        "demand_velocity_per_product": velocity.round(2).to_dict(),
        "seasonality": {
            "monthly_demand": {k: round(v, 2) for k, v in by_month.items()},
            "weekday_demand": {k: round(v, 2) for k, v in by_weekday.items()}
        },
        "rising_product_demand": rising_demand,
        "product_demand_matrix": matrix.astype(int).to_dict(),
        "data_key": cache_key
    })


# @api_view(["GET"])
# def product_revenue_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
#     df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"].astype(str).str.replace(",", ""), errors='coerce')
#     df = df.dropna(subset=["Created Date", "Net Extended Line Cost"])

#     try:
#         if start_date:
#             start = pd.to_datetime(start_date)
#             df = df[df["Created Date"] >= start]
#         if end_date:
#             end = pd.to_datetime(end_date)
#             df = df[df["Created Date"] <= end]
#     except Exception as e:
#         return Response({"error": f"Invalid date format: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No data found in the selected period."}, status=200)

#     try:
#         df["Month"] = df["Created Date"].dt.to_period("M").astype(str)
#         df["Weekday"] = df["Created Date"].dt.day_name()

#         top_products = df.groupby("Product Description")["Net Extended Line Cost"].sum().sort_values(ascending=False).head(10)
#         trend = df.groupby(df["Created Date"].dt.date)["Net Extended Line Cost"].sum().reset_index()
#         trend.columns = ["date", "revenue"]

#         store_revenue = (
#             df.groupby(["Store Name", "Product Description"])["Net Extended Line Cost"]
#             .sum().reset_index().sort_values(by="Net Extended Line Cost", ascending=False)
#         )

#         product_order_revenue = df.groupby(["Product Description", "Order Number"])["Net Extended Line Cost"].sum().reset_index()
#         revenue_yield = product_order_revenue.groupby("Product Description")["Net Extended Line Cost"].mean().sort_values(ascending=False).head(10)

#         by_month = df.groupby("Month")["Net Extended Line Cost"].sum().to_dict()
#         by_weekday = df.groupby("Weekday")["Net Extended Line Cost"].sum().sort_values(ascending=False).to_dict()

#         parsed_start = pd.to_datetime(parse_date(start_date)) if start_date else df["Created Date"].min()
#         parsed_end = pd.to_datetime(parse_date(end_date)) if end_date else df["Created Date"].max()
#         if not parsed_start or not parsed_end:
#             raise ValueError("Invalid start or end date for comparison")
#         period_length = parsed_end - parsed_start

#         prev_start = parsed_start - period_length
#         prev_end = parsed_start - timedelta(days=1)
#         prev_df = df[(df["Created Date"] >= prev_start) & (df["Created Date"] <= prev_end)]

#         recent_revenue = df.groupby("Product Description")["Net Extended Line Cost"].sum()
#         prev_revenue = prev_df.groupby("Product Description")["Net Extended Line Cost"].sum()

#         combined_revenue = pd.concat([recent_revenue, prev_revenue], axis=1, keys=["current", "previous"]).fillna(0)

#         combined_revenue["pct_change"] = combined_revenue.apply(safe_pct_change, axis=1)

#         rising_revenue = (
#             combined_revenue.sort_values(
#                 by="pct_change", ascending=False, key=lambda x: x.map(lambda v: float('-inf') if v == 0 else (float('inf') if v == "new product" else v))
#             )
#             .head(5)
#             .round(2)
#             .to_dict(orient="index")
#         )

#         matrix = df.pivot_table(index="Store Name", columns="Product Description", values="Net Extended Line Cost", aggfunc="sum", fill_value=0)

#     except Exception as e:
#         return Response({"error": f"Failed to compute revenue analysis: {str(e)}"}, status=500)

#     return Response({
#         "top_products_by_revenue": top_products.round(2).to_dict(),
#         "revenue_trend_over_time": trend.round(2).to_dict(orient="records"),
#         "store_product_revenue": store_revenue.round(2).to_dict(orient="records"),
#         "revenue_yield_per_product": revenue_yield.round(2).to_dict(),
#         "seasonality": {
#             "monthly_revenue": {k: round(v, 2) for k, v in by_month.items()},
#             "weekday_revenue": {k: round(v, 2) for k, v in by_weekday.items()}
#         },
#         "rising_product_revenue": rising_revenue,
#         "product_revenue_matrix": matrix.round(2).to_dict()
#     })

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
        combined_revenue["pct_change"] = combined_revenue.apply(safe_pct_change, axis=1)

        rising_revenue = (
            combined_revenue.sort_values(
                by="pct_change", ascending=False, key=lambda x: x.map(lambda v: float('-inf') if v == 0 else (float('inf') if v == "new product" else v))
            )
            .head(5)
            .round(2)
            .to_dict(orient="index")
        )

        matrix = df.pivot_table(index="Store Name", columns="Product Description", values="Net Extended Line Cost", aggfunc="sum", fill_value=0)

    except Exception as e:
        return Response({"error": f"Failed to compute revenue analysis: {str(e)}"}, status=500)

    # --- AI Insight + Forecast generation ---
    cache_key = f"product_revenue_analysis:{start_date or 'null'}:{end_date or 'null'}"
    period = "month"

    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    summary = {
        "top_products": top_products.head(5).round(2).to_dict(),
        "rising_revenue": rising_revenue,
        "revenue_yield": revenue_yield.head(5).round(2).to_dict(),
        "monthly_revenue": by_month
    }

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary, start_date, end_date, period, cache_key, "product_revenue_analysis")
    ).start()

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
        "product_revenue_matrix": matrix.round(2).to_dict(),
        "data_key": cache_key
    })


# @api_view(["GET"])
# def product_correlation_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     store_filter = request.GET.get("store")
#     sender_filter = request.GET.get("sender")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     try:
#         df = filter_by_date(df, start_date, end_date)
#         if store_filter:
#             df = df[df["Store Name"].str.lower() == store_filter.lower()]
#         if sender_filter:
#             df = df[df["Sender Name"].str.lower() == sender_filter.lower()]
#     except Exception as e:
#         return Response({"error": f"Error during filtering: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No data available for the selected filters."}, status=200)

#     try:
#         basket = df.groupby(["Order Number", "Product Description"])["Requested Qty"].sum().unstack(fill_value=0)
#         if basket.empty or basket.shape[1] < 2:
#             return Response({"message": "Not enough overlapping product orders to compute correlation."}, status=200)

#         binary_basket = (basket > 0).astype(int)
#         correlation_matrix = binary_basket.corr().fillna(0).round(3)

#         # Pairs
#         order_groups = df.groupby("Order Number")["Product Description"].apply(set)
#         pairs = []
#         for products in order_groups:
#             if len(products) > 1:
#                 pairs.extend(combinations(products, 2))

#         if not pairs:
#             return Response({"message": "Not enough co-occurring product pairs for analysis."}, status=200)

#         pair_counts = Counter(pairs)
#         most_common_pairs = dict(pair_counts.most_common(10))

#         # Affinity scores
#         affinity_scores = {}
#         for (prod_a, prod_b), count in pair_counts.items():
#             a_orders = binary_basket[prod_a].sum()
#             b_orders = binary_basket[prod_b].sum()
#             denominator = a_orders + b_orders - count
#             affinity = count / denominator if denominator else 0
#             affinity_scores[(prod_a, prod_b)] = round(affinity, 3)

#         # Central products
#         product_links = Counter()
#         for (a, b), count in pair_counts.items():
#             product_links[a] += count
#             product_links[b] += count
#         central_products = dict(product_links.most_common(10))

#     except Exception as e:
#         return Response({"error": f"Failed to process correlation analysis: {str(e)}"}, status=500)

#     return Response({
#         "most_common_product_pairs": {f"{a} & {b}": c for (a, b), c in most_common_pairs.items()},
#         "product_correlation_matrix": correlation_matrix.to_dict(),
#         "product_affinity_scores": {f"{a} & {b}": s for (a, b), s in sorted(affinity_scores.items(), key=lambda x: -x[1])[:10]},
#         "top_correlated_products_by_centrality": central_products
#     })

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

        order_groups = df.groupby("Order Number")["Product Description"].apply(set)
        pairs = []
        for products in order_groups:
            if len(products) > 1:
                pairs.extend(combinations(products, 2))

        if not pairs:
            return Response({"message": "Not enough co-occurring product pairs for analysis."}, status=200)

        pair_counts = Counter(pairs)
        most_common_pairs = dict(pair_counts.most_common(10))

        affinity_scores = {}
        for (prod_a, prod_b), count in pair_counts.items():
            a_orders = binary_basket[prod_a].sum()
            b_orders = binary_basket[prod_b].sum()
            denominator = a_orders + b_orders - count
            affinity = count / denominator if denominator else 0
            affinity_scores[(prod_a, prod_b)] = round(affinity, 3)

        product_links = Counter()
        for (a, b), count in pair_counts.items():
            product_links[a] += count
            product_links[b] += count
        central_products = dict(product_links.most_common(10))

    except Exception as e:
        return Response({"error": f"Failed to process correlation analysis: {str(e)}"}, status=500)

    # --- AI Insight & Forecast Thread ---
    cache_key = f"product_correlation_analysis:{start_date or 'null'}:{end_date or 'null'}"
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    summary = {
        "most_common_pairs": most_common_pairs,
        "affinity_scores": dict(sorted(affinity_scores.items(), key=lambda x: -x[1])[:5]),
        "central_products": central_products
    }

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=(summary, start_date, end_date, "month", cache_key, "product_correlation_analysis")
    ).start()

    return Response({
        "most_common_product_pairs": {f"{a} & {b}": c for (a, b), c in most_common_pairs.items()},
        "product_correlation_matrix": correlation_matrix.to_dict(),
        "product_affinity_scores": {
            f"{a} & {b}": s for (a, b), s in sorted(affinity_scores.items(), key=lambda x: -x[1])[:10]
        },
        "top_correlated_products_by_centrality": central_products,
        "data_key": cache_key
    })

# @api_view(["GET"])
# def product_trend_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     granularity = request.GET.get("interval", "M")  # D, W, M
#     store_filter = request.GET.get("store")
#     sender_filter = request.GET.get("sender")
#     product_filter = request.GET.get("product")
#     try:
#         top_n = int(request.GET.get("top", 5))
#     except ValueError:
#         return Response({"error": "Invalid top N value, must be an integer."}, status=400)

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     try:
#         df = filter_by_date(df, start_date, end_date)
#         if store_filter:
#             df = df[df["Store Name"].str.lower() == store_filter.lower()]
#         if sender_filter:
#             df = df[df["Sender Name"].str.lower() == sender_filter.lower()]
#         if product_filter:
#             df = df[df["Product Description"].str.lower().str.contains(product_filter.lower())]
#     except Exception as e:
#         return Response({"error": f"Error during filtering: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No data found for selected filters."}, status=200)

#     try:
#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#         df = df.dropna(subset=["Created Date"])
#         df["Requested Qty"] = pd.to_numeric(df["Requested Qty"], errors="coerce").fillna(0)
#         df["Net Extended Line Cost"] = pd.to_numeric(df["Net Extended Line Cost"], errors="coerce").fillna(0)
#         df["Order Number"] = df["Order Number"].astype(str)

#         df["Period"] = df["Created Date"].dt.to_period(granularity).astype(str)

#         revenue_trend = df.groupby(["Period", "Product Description"])["Net Extended Line Cost"].sum().reset_index()
#         quantity_trend = df.groupby(["Period", "Product Description"])["Requested Qty"].sum().reset_index()
#         freq_trend = df.groupby(["Period", "Product Description"])["Order Number"].nunique().reset_index()

#         revenue_pivot = revenue_trend.pivot(index="Period", columns="Product Description", values="Net Extended Line Cost").fillna(0)
#         quantity_pivot = quantity_trend.pivot(index="Period", columns="Product Description", values="Requested Qty").fillna(0)
#         freq_pivot = freq_trend.pivot(index="Period", columns="Product Description", values="Order Number").fillna(0)

#         product_totals = df.groupby("Product Description")["Net Extended Line Cost"].sum().sort_values(ascending=False)
#         top_products = product_totals.head(top_n).index.tolist()

#         if not top_products:
#             return Response({"message": "No product trends available for top products."}, status=200)

#         recent_periods = sorted(df["Period"].unique())[-3:]
#         trend_summary = {}
#         for product in top_products:
#             try:
#                 product_series = revenue_pivot[product].reindex(recent_periods, fill_value=0)
#                 direction = "increasing" if product_series.iloc[-1] > product_series.iloc[0] else "declining"
#                 trend_summary[product] = {
#                     "first_period": recent_periods[0],
#                     "last_period": recent_periods[-1],
#                     "change": round(product_series.iloc[-1] - product_series.iloc[0], 2),
#                     "direction": direction
#                 }
#             except Exception:
#                 trend_summary[product] = {
#                     "first_period": None,
#                     "last_period": None,
#                     "change": 0,
#                     "direction": "flat"
#                 }

#     except Exception as e:
#         return Response({"error": f"Failed to compute product trends: {str(e)}"}, status=500)

#     return Response({
#         "revenue_trend": revenue_pivot[top_products].round(2).to_dict(orient="index"),
#         "quantity_trend": quantity_pivot[top_products].round(2).to_dict(orient="index"),
#         "frequency_trend": freq_pivot[top_products].astype(int).to_dict(orient="index"),
#         "top_products": product_totals.head(top_n).round(2).to_dict(),
#         "trend_summary": trend_summary
#     })

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

    # --- Background AI Thread ---
    cache_key = f"product_trend_analysis:{start_date or 'null'}:{end_date or 'null'}:{granularity}"
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=({
            "top_products": list(product_totals.head(top_n).round(2).to_dict().items()),
            "trend_summary": trend_summary,
        }, start_date, end_date, granularity, cache_key, "product_trend_analysis")
    ).start()

    return Response({
        "revenue_trend": revenue_pivot[top_products].round(2).to_dict(orient="index"),
        "quantity_trend": quantity_pivot[top_products].round(2).to_dict(orient="index"),
        "frequency_trend": freq_pivot[top_products].astype(int).to_dict(orient="index"),
        "top_products": product_totals.head(top_n).round(2).to_dict(),
        "trend_summary": trend_summary,
        "data_key": cache_key
    })

# @api_view(["GET"])
# def order_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     granularity = request.GET.get("interval", "M")  # 'D', 'W', 'M'
#     store_filter = request.GET.get("store")
#     sender_filter = request.GET.get("sender")
#     product_filter = request.GET.get("product")

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     try:
#         df = filter_by_date(df, start_date, end_date)

#         if store_filter:
#             df = df[df["Store Name"].str.lower() == store_filter.lower()]

#         if sender_filter:
#             df = df[df["Sender Name"].str.lower() == sender_filter.lower()]

#         if product_filter:
#             df = df[df["Product Description"].str.lower().str.contains(product_filter.lower())]

#         if df.empty:
#             return Response({"message": "No data available for the selected filters."}, status=200)

#         # Ensure datetime
#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#         df = df.dropna(subset=["Created Date"])

#         # Basic metrics
#         total_orders = df["Order Number"].nunique()
#         total_value = pd.to_numeric(df["Net Extended Line Cost"], errors="coerce").sum()
#         unique_products = df["Product Description"].nunique()

#         avg_order_value = (
#             df.groupby("Order Number")["Net Extended Line Cost"]
#             .sum(min_count=1)
#             .mean()
#         )
#         items_per_order = (
#             df.groupby("Order Number")["Requested Qty"]
#             .sum(min_count=1)
#             .mean()
#         )

#         # Order volume trend
#         df["Period"] = df["Created Date"].dt.to_period(granularity).astype(str)
#         order_trend = df.groupby("Period")["Order Number"].nunique().to_dict()

#         # Top customers and stores
#         top_customers = (
#             df.groupby("Sender Name")["Order Number"]
#             .nunique()
#             .sort_values(ascending=False)
#             .head(5)
#             .to_dict()
#         )

#         top_stores = (
#             df.groupby("Store Name")["Order Number"]
#             .nunique()
#             .sort_values(ascending=False)
#             .head(5)
#             .to_dict()
#         )

#         # Fulfillment metrics
#         df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors="coerce")
#         df_fulfilled = df.dropna(subset=["Delivery Date"])
#         df_fulfilled["Fulfillment Days"] = (df_fulfilled["Delivery Date"] - df_fulfilled["Created Date"]).dt.days

#         fulfillment_stats = {
#             "average_days": round(df_fulfilled["Fulfillment Days"].mean(), 2)
#             if not df_fulfilled.empty else None,
#             "max_days": int(df_fulfilled["Fulfillment Days"].max())
#             if not df_fulfilled["Fulfillment Days"].empty else None,
#             "min_days": int(df_fulfilled["Fulfillment Days"].min())
#             if not df_fulfilled["Fulfillment Days"].empty else None,
#         }

#     except Exception as e:
#         return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

#     return Response({
#         "total_orders": total_orders,
#         "unique_products_ordered": unique_products,
#         "total_order_value": round(total_value, 2),
#         "average_order_value": round(avg_order_value or 0, 2),
#         "average_items_per_order": round(items_per_order or 0, 2),
#         "order_volume_trend": order_trend,
#         "top_customers_by_orders": top_customers,
#         "top_stores_by_orders": top_stores,
#         "fulfillment_stats": fulfillment_stats
#     })

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

    # --- AI Background Thread ---
    cache_key = f"order_analysis:{start_date or 'null'}:{end_date or 'null'}:{granularity}"
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=({
            "total_orders": total_orders,
            "unique_products": unique_products,
            "total_order_value": round(total_value, 2),
            "average_order_value": round(avg_order_value or 0, 2),
            "average_items_per_order": round(items_per_order or 0, 2),
            "order_volume_trend": order_trend,
            "top_customers": top_customers,
            "top_stores": top_stores,
            "fulfillment_stats": fulfillment_stats,
        }, start_date, end_date, granularity, cache_key, "order_analysis")
    ).start()

    return Response({
        "total_orders": total_orders,
        "unique_products_ordered": unique_products,
        "total_order_value": round(total_value, 2),
        "average_order_value": round(avg_order_value or 0, 2),
        "average_items_per_order": round(items_per_order or 0, 2),
        "order_volume_trend": order_trend,
        "top_customers_by_orders": top_customers,
        "top_stores_by_orders": top_stores,
        "fulfillment_stats": fulfillment_stats,
        "data_key": cache_key
    })

# @api_view(["GET"])
# def order_fulfillment_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     sla_param = request.GET.get("sla", 5)

#     try:
#         sla_days = int(sla_param)
#     except (TypeError, ValueError):
#         return Response({"error": "Invalid SLA value. It must be an integer."}, status=400)

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     try:
#         df = filter_by_date(df, start_date, end_date)

#         if df.empty:
#             return Response({"message": "No data found in the selected period."}, status=200)

#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#         df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors="coerce")
#         df = df.dropna(subset=["Created Date"])

#         total_orders = df["Order Number"].nunique()

#         # Canceled orders
#         canceled_orders_df = df[df["Delivery Date"].isna()]
#         canceled_orders = canceled_orders_df["Order Number"].nunique()
#         cancellation_rate = round((canceled_orders / total_orders) * 100, 2) if total_orders else 0

#         # Fulfilled orders
#         fulfilled_df = df.dropna(subset=["Delivery Date"]).copy()
#         fulfilled_df["Fulfillment Days"] = (fulfilled_df["Delivery Date"] - fulfilled_df["Created Date"]).dt.days

#         if fulfilled_df.empty:
#             return Response({"message": "No fulfilled orders in this period."}, status=200)

#         # Fulfillment stats
#         stats = fulfilled_df["Fulfillment Days"].describe().round(2).to_dict()
#         stats["std"] = round(fulfilled_df["Fulfillment Days"].std(), 2)

#         # SLA compliance
#         within_sla = (fulfilled_df["Fulfillment Days"] <= sla_days).sum()
#         total_fulfilled_orders = fulfilled_df["Order Number"].nunique()
#         sla_pct = round((within_sla / total_fulfilled_orders) * 100, 2) if total_fulfilled_orders else 0

#         # Delivery efficiency
#         delivery_rate = round((total_fulfilled_orders / total_orders) * 100, 2) if total_orders else 0
#         delivery_efficiency_score = round((delivery_rate * sla_pct) / 100, 2)

#         # Performance by store and sender
#         by_store = fulfilled_df.groupby("Store Name")["Fulfillment Days"].mean().round(2).sort_values().to_dict()
#         by_sender = fulfilled_df.groupby("Sender Name")["Fulfillment Days"].mean().round(2).sort_values().to_dict()

#         # Top delays
#         delayed = fulfilled_df[fulfilled_df["Fulfillment Days"] > sla_days]
#         top_delays = delayed.sort_values("Fulfillment Days", ascending=False)
#         top_delays = top_delays[["Order Number", "Store Name", "Sender Name", "Fulfillment Days"]].head(5).to_dict(orient="records")

#         # Distribution
#         dist = fulfilled_df["Fulfillment Days"].value_counts().sort_index().to_dict()

#         return Response({
#             "fulfillment_statistics": stats,
#             "percent_within_sla": sla_pct,
#             "delivery_rate": delivery_rate,
#             "delivery_efficiency_score": delivery_efficiency_score,
#             "cancellation_rate": cancellation_rate,
#             "fulfillment_distribution": dist,
#             "top_delayed_orders": top_delays,
#             "average_fulfillment_by_store": by_store,
#             "average_fulfillment_by_sender": by_sender
#         })

#     except Exception as e:
#         return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

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

        # --- AI Background Processing ---
        cache_key = f"order_fulfillment_analysis:{start_date or 'null'}:{end_date or 'null'}:{sla_days}"
        cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
        cache.set(cache_key + ":insight", "Processing...", timeout=3600)
        cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

        threading.Thread(
            target=generate_insight_and_forecast_background,
            args=({
                "fulfillment_statistics": stats,
                "percent_within_sla": sla_pct,
                "delivery_rate": delivery_rate,
                "delivery_efficiency_score": delivery_efficiency_score,
                "cancellation_rate": cancellation_rate,
                "fulfillment_distribution": dist,
                "top_delayed_orders": top_delays,
                "average_fulfillment_by_store": by_store,
                "average_fulfillment_by_sender": by_sender
            }, start_date, end_date, sla_days, cache_key, "order_fulfillment_analysis")
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
            "data_key": cache_key
        })

    except Exception as e:
        return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

# @api_view(["GET"])
# def order_calculation_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     threshold_param = request.GET.get("threshold", 1000)

#     try:
#         high_value_threshold = float(threshold_param)
#     except (TypeError, ValueError):
#         return Response({"error": "Invalid threshold. It must be a numeric value."}, status=400)

#     try:
#         df = load_data()
#     except Exception as e:
#         return Response({"error": f"Failed to load data: {str(e)}"}, status=500)

#     try:
#         df = filter_by_date(df, start_date, end_date)

#         if df.empty:
#             return Response({"message": "No order data found in this period."}, status=200)

#         # Ensure datetime conversion
#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
#         df = df.dropna(subset=["Created Date"])

#         # Aggregate order-level data
#         order_value = df.groupby("Order Number")["Net Extended Line Cost"].sum()
#         order_items = df.groupby("Order Number")["Requested Qty"].sum()
#         order_products = df.groupby("Order Number")["Product Description"].nunique()

#         total_orders = order_value.count()
#         total_lines = df.shape[0]
#         avg_order_value = round(order_value.mean(), 2) if not order_value.empty else 0
#         avg_items = round(order_items.mean(), 2) if not order_items.empty else 0
#         avg_products = round(order_products.mean(), 2) if not order_products.empty else 0

#         # Identify high-value and low-value orders
#         high_value_orders = order_value[order_value > high_value_threshold]
#         low_value_orders = order_value[order_value <= high_value_threshold]

#         high_value_sample = high_value_orders.sort_values(ascending=False).head(5).round(2).to_dict()
#         low_value_sample = low_value_orders.sort_values().head(5).round(2).to_dict()

#         high_pct = round((len(high_value_orders) / total_orders) * 100, 2) if total_orders else 0
#         low_pct = round((len(low_value_orders) / total_orders) * 100, 2) if total_orders else 0

#         # Order frequency trend
#         df["Order Date"] = df["Created Date"].dt.to_period("M").astype(str)
#         order_freq = df.groupby("Order Date")["Order Number"].nunique().to_dict()

#         # Order value distribution
#         bins = [0, 250, 500, 1000, 2000, float("inf")]
#         labels = ["<250", "250-500", "500-1k", "1k-2k", ">2k"]
#         df["Order Value"] = df.groupby("Order Number")["Net Extended Line Cost"].transform("sum")
#         df["Value Range"] = pd.cut(df["Order Value"], bins=bins, labels=labels)
#         distribution = df["Value Range"].value_counts().sort_index().to_dict()

#         return Response({
#             "order_summary": {
#                 "total_orders": total_orders,
#                 "total_order_lines": total_lines,
#                 "avg_order_value": avg_order_value,
#                 "avg_items_per_order": avg_items,
#                 "avg_product_types_per_order": avg_products
#             },
#             "high_value_orders": {
#                 "count": len(high_value_orders),
#                 "percentage": high_pct,
#                 "sample": high_value_sample
#             },
#             "low_value_orders": {
#                 "count": len(low_value_orders),
#                 "percentage": low_pct,
#                 "sample": low_value_sample
#             },
#             "order_value_distribution": distribution,
#             "order_frequency_trend": order_freq
#         })

#     except Exception as e:
#         return Response({"error": f"Analysis failed: {str(e)}"}, status=500)

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

        df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        df = df.dropna(subset=["Created Date"])

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
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     today = datetime.today().date()

#     try:
#         df = load_data()
#         df = filter_by_date(df, start_date, end_date)
#         df["Created Date"] = pd.to_datetime(df["Created Date"], errors='coerce')
#         df.dropna(subset=["Created Date", "Sender Name"], inplace=True)
#     except Exception as e:
#         return Response({"error": f"Data error: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No transaction data found for the selected period."})

#     # ===== RFM Calculation =====
#     rfm = df.groupby("Sender Name").agg({
#         "Created Date": lambda x: (today - x.max().date()).days,   # Recency
#         "Order Number": "nunique",                                 # Frequency
#         "Net Extended Line Cost": "sum"                            # Monetary
#     }).reset_index()

#     rfm.columns = ["Customer", "Recency", "Frequency", "Monetary"]
#     rfm["Segment"] = pd.qcut(rfm["Monetary"], 4, labels=["Low", "Mid-Low", "Mid-High", "High"])

#     # ===== Revenue Over Time by Customer =====
#     df["Period"] = df["Created Date"].dt.to_period("M").astype(str)
#     revenue_time = df.groupby(["Period", "Sender Name"])["Net Extended Line Cost"].sum().reset_index()
#     revenue_pivot = revenue_time.pivot(index="Period", columns="Sender Name", values="Net Extended Line Cost").fillna(0).round(2)
#     revenue_over_time = revenue_pivot.to_dict(orient="index")

#     # ===== Top Growing Customers by Revenue (Last 2 Periods) =====
#     if len(revenue_pivot.index) >= 2:
#         latest_two = revenue_pivot.iloc[-2:]
#         revenue_diff = latest_two.diff().iloc[-1].sort_values(ascending=False)
#         top_growing_customers = revenue_diff.head(5).round(2).to_dict()
#     else:
#         top_growing_customers = {}

#     # ===== Customer Churn Indication =====
#     churn_threshold = 30  # days without purchase
#     churned_customers = rfm[rfm["Recency"] > churn_threshold].sort_values("Recency", ascending=False)
#     churn_list = churned_customers[["Customer", "Recency", "Monetary"]].head(10).round(2).to_dict(orient="records")

#     return Response({
#         "customer_rfm": rfm.round(2).to_dict(orient="records"),
#         "summary": {
#             "total_customers": rfm.shape[0],
#             "high_value_customers": int((rfm["Segment"] == "High").sum()),
#             "low_value_customers": int((rfm["Segment"] == "Low").sum()),
#         },
#         "revenue_over_time": revenue_over_time,
#         "top_growing_customers": top_growing_customers,
#         "churned_customers": churn_list
#     })

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

# @api_view(["GET"])
# def customer_purchase_pattern_analysis(request):
#     start_date = request.GET.get("start_date")
#     end_date = request.GET.get("end_date")
#     store = request.GET.get("store")
#     sender = request.GET.get("sender")

#     try:
#         df = load_data()
#         df = filter_by_date(df, start_date, end_date)
#         df.dropna(subset=["Created Date", "Sender Name", "Store Name", "Order Number"], inplace=True)

#         df["Store Name"] = df["Store Name"].astype(str).str.strip()
#         df["Sender Name"] = df["Sender Name"].astype(str).str.strip()

#         if store:
#             df = df[df["Store Name"].str.lower() == store.lower()]

#         if sender:
#             df = df[df["Sender Name"].str.lower() == sender.lower()]

#     except Exception as e:
#         return Response({"error": f"Data error: {str(e)}"}, status=400)

#     if df.empty:
#         return Response({"message": "No transaction data found for the selected filters."})

#     today = datetime.today().date()
#     df["Date"] = df["Created Date"].dt.date
#     df["Weekday"] = df["Created Date"].dt.day_name()
#     df["Hour"] = df["Created Date"].dt.hour

#     # Determine grouping
#     if store and sender:
#         group_by = ["Store Name", "Sender Name"]
#         group_label = lambda row: f"{row['Store Name']} | {row['Sender Name']}"
#     elif store:
#         group_by = ["Store Name"]
#         group_label = lambda row: row["Store Name"]
#     else:
#         group_by = ["Sender Name"]
#         group_label = lambda row: row["Sender Name"]

#     # 1. Aggregate Summary
#     customer_summary = df.groupby(group_by).agg(
#         total_orders=("Order Number", "nunique"),
#         total_revenue=("Net Extended Line Cost", "sum"),
#         total_quantity=("Requested Qty", "sum"),
#         first_purchase=("Date", "min"),
#         last_purchase=("Date", "max"),
#         distinct_products=("Product Description", "nunique")
#     ).reset_index()

#     customer_summary["days_since_last_purchase"] = customer_summary["last_purchase"].apply(lambda x: (today - x).days)
#     customer_summary["avg_order_value"] = (customer_summary["total_revenue"] / customer_summary["total_orders"]).round(2)
#     customer_summary["avg_items_per_order"] = (customer_summary["total_quantity"] / customer_summary["total_orders"]).round(2)

#     # 2. Purchase Frequency Patterns
#     order_dates = df.groupby(group_by + ["Order Number"])["Date"].min().reset_index()
#     order_diffs = order_dates.groupby(group_by)["Date"].apply(lambda x: x.sort_values().diff().dt.days.dropna())
#     avg_days_between_orders = order_diffs.groupby(level=0).mean().round(2)
#     repeat_rate = order_dates.groupby(group_by)["Order Number"].count().apply(lambda x: 1 if x > 1 else 0)

#     customer_summary.set_index(group_by, inplace=True)
#     customer_summary["avg_days_between_orders"] = avg_days_between_orders
#     customer_summary["is_repeater"] = repeat_rate

#     # 3. Top Products
#     top_products = (
#         df.groupby(group_by + ["Product Description"])["Requested Qty"]
#         .sum().reset_index()
#         .sort_values(group_by + ["Requested Qty"], ascending=[True]*len(group_by) + [False])
#     )
#     top_products = top_products.groupby(group_by).head(3).groupby(group_by)["Product Description"].apply(list)
#     customer_summary["top_products"] = top_products

#     # 4. Time Patterns
#     weekday_pref = df.groupby(group_by + ["Weekday"])["Order Number"].nunique().reset_index()
#     weekday_pref = weekday_pref.sort_values(group_by + ["Order Number"], ascending=[True]*len(group_by) + [False])
#     top_weekday = weekday_pref.groupby(group_by).first().reset_index()
#     customer_summary["top_order_day"] = pd.Series(
#         top_weekday.set_index(pd.MultiIndex.from_frame(top_weekday[group_by]))["Weekday"]
#     )

#     hour_pref = df.groupby(group_by + ["Hour"])["Order Number"].nunique().reset_index()
#     hour_pref = hour_pref.sort_values(group_by + ["Order Number"], ascending=[True]*len(group_by) + [False])
#     top_hour = hour_pref.groupby(group_by).first().reset_index()
#     customer_summary["top_order_hour"] = pd.Series(
#         top_hour.set_index(pd.MultiIndex.from_frame(top_hour[group_by]))["Hour"]
#     )

#     # 5. Segmentation
#     def segment(row):
#         if row["total_orders"] == 1:
#             return "New"
#         elif row["avg_days_between_orders"] and row["avg_days_between_orders"] < 14:
#             return "Frequent"
#         elif row["is_repeater"]:
#             return "Returning"
#         return "One-time"

#     customer_summary["segment"] = customer_summary.apply(segment, axis=1)

#     # 6. Timeline
#     timeline = df.groupby(group_by + ["Date"])["Order Number"].nunique().reset_index()
#     customer_timeline = (
#         timeline.groupby(group_by)
#         .apply(lambda x: x.drop(columns=group_by).sort_values("Date").to_dict(orient="records"))
#         .to_dict()
#     )

#     # Final Output
#     customer_summary = customer_summary.reset_index()
#     customer_summary["group"] = customer_summary.apply(group_label, axis=1)
#     result = customer_summary.round(2).to_dict(orient="records")

#     return Response({
#         "customer_purchase_patterns": result,
#         "customer_order_timeline": {k if isinstance(k, str) else " | ".join(k): v for k, v in customer_timeline.items()},
#         "summary": {
#             "total_customers": len(result),
#             "frequent_customers": int((customer_summary["segment"] == "Frequent").sum()),
#             "returning_customers": int((customer_summary["segment"] == "Returning").sum()),
#             "new_customers": int((customer_summary["segment"] == "New").sum())
#         }
#     })

@api_view(["GET"])
def customer_purchase_pattern_analysis(request):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    store = request.GET.get("store")
    sender = request.GET.get("sender")

    try:
        df = load_data()
        df = filter_by_date(df, start_date, end_date)
        df.dropna(subset=["Created Date", "Sender Name", "Store Name", "Order Number"], inplace=True)

        df["Store Name"] = df["Store Name"].astype(str).str.strip()
        df["Sender Name"] = df["Sender Name"].astype(str).str.strip()

        if store:
            df = df[df["Store Name"].str.lower() == store.lower()]
        if sender:
            df = df[df["Sender Name"].str.lower() == sender.lower()]
    except Exception as e:
        return Response({"error": f"Data error: {str(e)}"}, status=400)

    if df.empty:
        return Response({"message": "No transaction data found for the selected filters."}, status=200)

    today = datetime.today().date()
    df["Date"] = df["Created Date"].dt.date
    df["Weekday"] = df["Created Date"].dt.day_name()
    df["Hour"] = df["Created Date"].dt.hour

    # Determine grouping
    if store and sender:
        group_by = ["Store Name", "Sender Name"]
        group_label = lambda row: f"{row['Store Name']} | {row['Sender Name']}"
    elif store:
        group_by = ["Store Name"]
        group_label = lambda row: row["Store Name"]
    else:
        group_by = ["Sender Name"]
        group_label = lambda row: row["Sender Name"]

    # 1. Aggregate Summary
    customer_summary = df.groupby(group_by).agg(
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
    order_dates = df.groupby(group_by + ["Order Number"])["Date"].min().reset_index()
    order_diffs = order_dates.groupby(group_by)["Date"].apply(lambda x: x.sort_values().diff().dt.days.dropna())
    avg_days_between_orders = order_diffs.groupby(level=0).mean().round(2)
    repeat_rate = order_dates.groupby(group_by)["Order Number"].count().apply(lambda x: 1 if x > 1 else 0)

    customer_summary.set_index(group_by, inplace=True)
    customer_summary["avg_days_between_orders"] = avg_days_between_orders
    customer_summary["is_repeater"] = repeat_rate

    # 3. Top Products
    top_products = (
        df.groupby(group_by + ["Product Description"])["Requested Qty"]
        .sum().reset_index()
        .sort_values(group_by + ["Requested Qty"], ascending=[True]*len(group_by) + [False])
    )
    top_products = top_products.groupby(group_by).head(3).groupby(group_by)["Product Description"].apply(list)
    customer_summary["top_products"] = top_products

    # 4. Time Patterns
    weekday_pref = df.groupby(group_by + ["Weekday"])["Order Number"].nunique().reset_index()
    weekday_pref = weekday_pref.sort_values(group_by + ["Order Number"], ascending=[True]*len(group_by) + [False])
    top_weekday = weekday_pref.groupby(group_by).first().reset_index()
    customer_summary["top_order_day"] = pd.Series(
        top_weekday.set_index(pd.MultiIndex.from_frame(top_weekday[group_by]))["Weekday"]
    )

    hour_pref = df.groupby(group_by + ["Hour"])["Order Number"].nunique().reset_index()
    hour_pref = hour_pref.sort_values(group_by + ["Order Number"], ascending=[True]*len(group_by) + [False])
    top_hour = hour_pref.groupby(group_by).first().reset_index()
    customer_summary["top_order_hour"] = pd.Series(
        top_hour.set_index(pd.MultiIndex.from_frame(top_hour[group_by]))["Hour"]
    )

    # 5. Segmentation
    def segment(row):
        if row["total_orders"] == 1:
            return "New"
        elif row["avg_days_between_orders"] and row["avg_days_between_orders"] < 14:
            return "Frequent"
        elif row["is_repeater"]:
            return "Returning"
        return "One-time"

    customer_summary["segment"] = customer_summary.apply(segment, axis=1)

    # 6. Timeline
    timeline = df.groupby(group_by + ["Date"])["Order Number"].nunique().reset_index()
    customer_timeline = (
        timeline.groupby(group_by)
        .apply(lambda x: x.drop(columns=group_by).sort_values("Date").to_dict(orient="records"))
        .to_dict()
    )

    # Final Output
    customer_summary = customer_summary.reset_index()
    customer_summary["group"] = customer_summary.apply(group_label, axis=1)
    result = customer_summary.round(2).to_dict(orient="records")

    # === AI Background Task Trigger ===
    cache_key = f"customer_purchase_pattern_analysis:{start_date or 'null'}:{end_date or 'null'}:{store or 'all'}:{sender or 'all'}"
    cache.set(cache_key + ":status", {"insight": "processing", "forecast": "processing"}, timeout=3600)
    cache.set(cache_key + ":insight", "Processing...", timeout=3600)
    cache.set(cache_key + ":forecast", "Processing...", timeout=3600)

    threading.Thread(
        target=generate_insight_and_forecast_background,
        args=({
            "customer_purchase_patterns": result,
            "summary": {
                "total_customers": len(result),
                "frequent_customers": int((customer_summary["segment"] == "Frequent").sum()),
                "returning_customers": int((customer_summary["segment"] == "Returning").sum()),
                "new_customers": int((customer_summary["segment"] == "New").sum())
            }
        }, start_date, end_date, None, cache_key, "customer_purchase_pattern_analysis")
    ).start()

    return Response({
        "customer_purchase_patterns": result,
        "customer_order_timeline": {k if isinstance(k, str) else " | ".join(k): v for k, v in customer_timeline.items()},
        "summary": {
            "total_customers": len(result),
            "frequent_customers": int((customer_summary["segment"] == "Frequent").sum()),
            "returning_customers": int((customer_summary["segment"] == "Returning").sum()),
            "new_customers": int((customer_summary["segment"] == "New").sum())
        },
        "data_key": cache_key
    })


@api_view(['GET'])
def list_all_products(request):
    try:
        df = load_data()

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
