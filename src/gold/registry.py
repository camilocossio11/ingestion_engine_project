from aggregators.climate_impact import ClimateImpact
from aggregators.order_fulfillment_performance import OrderFulfillmentPerformance
from aggregators.sales_volume import SalesVolume

AGGREGATORS_REGISTRY = {
    "climate_impact": ClimateImpact,
    "order_fulfillment_performance": OrderFulfillmentPerformance,
    "sales_volume": SalesVolume,
}
