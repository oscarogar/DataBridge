from datetime import datetime, timedelta
from functools import lru_cache
import os
import pandas as pd


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
    elif view_name == "retail_inventory_analysis":
        insight_prompt = (
            f"Analyze this retail inventory dataset between {start_date} and {end_date}. "
            f"Provide business insights about inventory levels, warehouse utilization, operational costs, "
            f"forecast accuracy, and demand fulfillment efficiency across stores and suppliers. "
            f"Identify which stores are overstocked or understocked, where logistics or operational costs are high, "
            f"and how lead time impacts overall performance. "
            f"Summarize actionable recommendations to optimize inventory levels, reduce costs, and improve demand forecasting."
        )
        forecast_prompt = (
            f"Using inventory data trends from {start_date} to {end_date}, "
            f"forecast expected inventory performance for the next {period}. "
            f"Predict changes in inventory levels, warehouse utilization, logistics costs, and lead time. "
            f"Estimate how forecast accuracy might improve or decline and recommend proactive supply chain strategies "
            f"to balance stock levels and enhance fulfillment efficiency."
        )

    elif view_name == "promotion_analysis":
        insight_prompt = (
            f"Analyze the promotional campaign data between {start_date} and {end_date}. "
            f"Explain how each marketing campaign performed in terms of total sales, profit margin, and discount impact. "
            f"Identify which campaigns or stores generated the highest sales volumes and which offered the best profitability. "
            f"Highlight top-performing products within campaigns, effectiveness of discounts, and possible signs of promotion fatigue or underperformance. "
            f"Summarize actionable insights on campaign efficiency, store participation, and sales uplift. The currency is ZAR."
        )
        forecast_prompt = (
            f"Based on promotion performance data from {start_date} to {end_date}, "
            f"forecast the expected outcomes for upcoming promotional periods. "
            f"Predict which campaign types, discount levels, or product categories are likely to drive sales and profit growth in the next {period}. "
            f"Include projections on sales uplift, margin stability, and potential optimization of campaign strategies to maximize ROI."
        )

    elif view_name == "operations_metrics":
        insight_prompt = (
            f"Analyze the full operational performance dataset between {start_date} and {end_date}. "
            f"Provide a structured insight across these key dimensions: sales performance, logistics efficiency, "
            f"inventory management, customer satisfaction, workforce productivity, and marketing effectiveness. "
            f"Discuss how these metrics interrelate — for example, how logistics or inventory may influence sales "
            f"or customer experience. Highlight bottlenecks, inefficiencies, or strong performers. "
            f"Include specific observations such as margin quality, cost-to-sales ratios, delivery success rates, "
            f"CSAT sentiment, employee contribution patterns, and supplier quality trends. "
            f"Use clear business reasoning to explain what these metrics imply about overall operational health, "
            f"profitability, and sustainability. Currency is ZAR."
        )

        forecast_prompt = (
            f"Based on the operational performance data from {start_date} to {end_date}, "
            f"forecast how the next {period} might evolve across the same dimensions — sales, logistics, inventory, "
            f"customer satisfaction, workforce, and marketing. Predict trends such as potential margin shifts, "
            f"lead time changes, warehouse utilization improvements, or fluctuations in customer sentiment and "
            f"supplier reliability. Provide a concise forward-looking summary with expected risks and opportunities, "
            f"and suggest operational focus areas to maintain profitability and efficiency. Currency is ZAR."
        )
    elif view_name == "finance_analytics":
        insight_prompt = (
            f"Perform a comprehensive financial performance analysis for the dataset between {start_date} and {end_date}. "
            f"Focus on profitability, efficiency, and liquidity across multiple financial dimensions such as revenue, "
            f"cost of goods sold (COGS), gross profit, operating expenses, logistics costs, and EBITDA. "
            f"Highlight trends, identify cost drivers, and interpret margin performance — including gross, operating, "
            f"and EBITDA margins. Assess how efficiently the organization is managing resources, "
            f"using metrics like inventory turnover, average lead time, and delivery efficiency. "
            f"Discuss regional and channel-based profitability differences, pinpoint underperforming segments, "
            f"and highlight any emerging risks or opportunities. Include cashflow and payment method insights, "
            f"explaining their implications for liquidity and working capital health. "
            f"Summarize overall financial stability, resilience, and sustainability with strategic recommendations. "
            f"Currency is ZAR."
        )

        forecast_prompt = (
            f"Using financial data from {start_date} to {end_date}, forecast expected trends for the next {period}. "
            f"Predict how revenue, cost of goods sold, gross profit, and key financial ratios might evolve based on "
            f"recent operational patterns. Estimate future margin movement (gross, operating, and EBITDA) and outline "
            f"probable shifts in expense structure, logistics cost efficiency, and cashflow composition. "
            f"Project liquidity position and working capital outlook using inferred cash inflow and outflow patterns. "
            f"Highlight potential profitability risks, cost inflation zones, or margin expansion opportunities. "
            f"Conclude with data-driven recommendations for financial optimization, capital allocation, and risk management. "
            f"Currency is ZAR."
        )
    elif view_name == "customer_experience_analytics":
        insight_prompt = (
            f"Perform an in-depth Customer Experience (CX) analysis for data between {start_date} and {end_date}. "
            f"Evaluate customer satisfaction (CSAT), feedback sentiment, service delivery performance, and campaign impact. "
            f"Identify the main drivers of positive and negative experiences — such as delivery speed, lead time, product category, "
            f"or communication effectiveness. Analyze patterns in on-time delivery, feedback frequency, and customer sentiment "
            f"across regions, sales channels, and customer segments. "
            f"Highlight recurring pain points (e.g., slow delivery or poor service) and strong satisfaction factors (e.g., reliability, value, staff interaction). "
            f"Assess campaign effectiveness and channel performance in shaping customer perception. "
            f"Provide actionable insights that explain what influences customer loyalty and how satisfaction trends connect "
            f"to operational quality and marketing strategy. Currency is ZAR."
        )

        forecast_prompt = (
            f"Based on customer experience data from {start_date} to {end_date}, forecast how customer satisfaction, feedback sentiment, "
            f"and service delivery metrics are likely to evolve in the next {period}. "
            f"Predict trends such as potential CSAT improvement or decline, lead time reduction or increase, and shifts in on-time delivery rates. "
            f"Anticipate which customer segments or regions may show higher satisfaction or emerging dissatisfaction. "
            f"Include a projection of campaign effectiveness and feedback sentiment mix (positive vs. negative). "
            f"Conclude with clear recommendations on how to improve customer experience, enhance satisfaction, "
            f"and prevent churn — for example, optimizing delivery times, communication quality, or post-service engagement. "
            f"Currency is ZAR."
        )
    elif view_name == "human_resource_analytics":
        insight_prompt = (
            f"Conduct a full human resource analytics review for data between {start_date} and {end_date}. "
            f"Provide insight into workforce composition, attendance trends, compensation structure, and employee performance. "
            f"Evaluate total headcount, tenure distribution, and department strength to identify capacity balance and workforce stability. "
            f"Analyze attendance behavior, overtime patterns, and turnover rate to detect productivity strengths or absenteeism risks. "
            f"Review payroll distribution and salary averages to highlight compensation equity, labor cost efficiency, "
            f"and return on payroll investment. Examine performance by employee, department, and region to spot top performers "
            f"and underperforming areas. Include leadership and engagement interpretations — such as morale, motivation, and team alignment "
            f"inferred from attendance and performance trends. "
            f"Conclude with strategic HR insights that connect workforce behavior to operational or financial outcomes. Currency is ZAR."
        )

        forecast_prompt = (
            f"Using HR data from {start_date} to {end_date}, forecast workforce and performance trends for the next {period}. "
            f"Predict changes in headcount stability, turnover rate, and average attendance. Estimate likely shifts in compensation costs, "
            f"salary averages, and performance distribution across departments or regions. "
            f"Anticipate potential talent retention risks, workforce fatigue, or productivity improvement opportunities based on historical patterns. "
            f"Highlight departments or roles most likely to experience turnover or require upskilling. "
            f"Conclude with actionable recommendations for workforce planning, retention strategy, and performance optimization. "
            f"Currency is ZAR."
        )
    elif view_name == "procurement_analytics":
        insight_prompt = (
            f"Perform a comprehensive procurement performance analysis for data between {start_date} and {end_date}. "
            f"Evaluate supplier performance, delivery reliability, procurement cost efficiency, and sourcing risk. "
            f"Analyze total procurement spend, cost per supplier, and supplier concentration to assess cost distribution "
            f"and exposure to supplier dependency. Examine lead time consistency, on-time delivery rates, and forecast accuracy "
            f"to gauge operational reliability. Identify the most and least cost-efficient suppliers using the supplier cost efficiency index "
            f"and analyze how volatility in unit prices affects budget stability. Review supplier diversity and HHI score to assess competition "
            f"and resilience in the supplier base. Discuss stockout frequency and cycle efficiency to reveal supply chain responsiveness. "
            f"Highlight areas of overspending, delivery delay, or procurement inefficiency, and provide actionable insights "
            f"for optimizing sourcing strategies, improving reliability, and enhancing supplier relationships. Currency is ZAR."
        )

        forecast_prompt = (
            f"Using procurement data from {start_date} to {end_date}, forecast expected trends in sourcing performance and cost efficiency "
            f"for the next {period}. Predict how total procurement costs, average supplier cost, and lead times might evolve. "
            f"Estimate potential changes in supplier reliability, forecast accuracy, and volatility in unit prices. "
            f"Project future risks of stockouts, longer delivery cycles, or reduced supplier diversity. "
            f"Highlight potential opportunities for negotiation, vendor consolidation, or diversification based on observed trends. "
            f"Conclude with strategic recommendations on cost control, supplier management, and supply chain resilience for the upcoming period. "
            f"Currency is ZAR."
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
