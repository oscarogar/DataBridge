from django.urls import path
from . import views

urlpatterns = [
    path('total-sales/', views.sales_analytics, name='total_sales'),
    path('sales-trend/', views.sales_trend_analytics, name='sales_trend'),
    path('profit-analysis/', views.profit_margin_analytics),
    path('cost-analysis/', views.cost_analysis, name='cost_analysis'),
    path("overview/", views.sales_summary),
    
    path("transaction-summary/", views.transaction_summary),
    path("top-performing-businesses/", views.transaction_entities_analysis),
    path("timing-analysis/", views.transaction_timing_analysis),
    
    
    path("product-demand-analysis/", views.product_demand_analysis),
    path("product-revenues/", views.product_revenue_analysis),
    path("product-correlation/", views.product_correlation_analysis),
    path("product-trend-analysis/", views.product_trend_analysis),
    
    path("order-analysis/", views.order_analysis),
    path("order-fulfillment-analysis/", views.order_fulfillment_analysis),
    # path("order-calculation-analysis/", views.order_calculation_analysis),
    
    path("customer-segmentation-analysis/", views.customer_segmentation_analysis),
    path('customer-buying-patterns/', views.customer_purchase_pattern_analysis),
    
    # path("list-products/", views.list_all_products),
    path("inventory/", views.retail_inventory_analysis),
    path('promotions/', views.promotion_analysis),
    
    path('get-insights-and-analytics/', views.get_sales_insight_result),
    
    # path("invoice/trend/conversion/", views.invoice_trend_and_conversion),
    # path('product/performance/', views.product_performance_analysis),
    # path('operations/metrics/', views.operations_metrics),
    
    
    
    
    
    path("python-version/", views.python_version_view),
    
]