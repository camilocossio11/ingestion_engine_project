from .aggregators import ClimateImpact, OrderFulfillmentPerformance, SalesVolume

AGGREGATORS_REGISTRY = {
    "climate_impact": ClimateImpact,
    "order_fulfillment_performance": OrderFulfillmentPerformance,
    "sales_volume": SalesVolume,
}
