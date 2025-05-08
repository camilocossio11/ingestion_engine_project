from .processors import (
    ClimateProcessor,
    InventoryProcessor,
    IoTProcessor,
    LogisticsProcessor,
    SalesProcessor,
)

PROCESSOR_REGISTRY = {
    "online_sales": SalesProcessor,
    "logistics": LogisticsProcessor,
    "inventory": InventoryProcessor,
    "climate": ClimateProcessor,
    "iot_sensors": IoTProcessor,
}
