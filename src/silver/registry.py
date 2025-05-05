from processors.climate_processor import ClimateProcessor
from processors.inventory_processor import InventoryProcessor
from processors.iot_processor import IoTProcessor
from processors.logistics_processor import LogisticsProcessor
from processors.sales_processor import SalesProcessor

PROCESSOR_REGISTRY = {
    "online_sales": SalesProcessor,
    "logistics": LogisticsProcessor,
    "inventory": InventoryProcessor,
    "climate": ClimateProcessor,
    "iot_sensors": IoTProcessor,
}
