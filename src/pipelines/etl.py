from domain.events.market_events import MarketDataEvent
from domain.processing.layers.preprocessing import ETLPipeline, Transformer
from framework.patterns.disruptor import EventProcessor


class MarketDataETL(ETLPipeline):
    def __init__(self, config: dict):
        super().__init__("market_data_etl", config)

    async def extract(self, staged_data):
        """Extract from staging area"""
        # Validate data schema
        validated = await self.validate_schema(staged_data)

        # Extract relevant fields
        return {
            "symbol": validated["symbol"],
            "timestamp": validated["timestamp"],
            "price": float(validated["price"]),
            "volume": int(validated["volume"]),
            "source": validated.get("source", "unknown"),
        }

    async def transform(self, extracted_data):
        """Transform data"""
        # Normalize timestamps
        extracted_data["timestamp"] = self.normalize_timestamp(
            extracted_data["timestamp"]
        )

        # Calculate derived fields
        extracted_data["vwap"] = await self.calculate_vwap(
            extracted_data["symbol"], extracted_data["price"], extracted_data["volume"]
        )

        # Add technical indicators
        extracted_data["indicators"] = await self.calculate_indicators(extracted_data)

        return extracted_data

    async def load(self, transformed_data):
        """Load into ODS and event queue"""
        # Store in ODS
        await self.ods.insert(transformed_data)

        # Publish to event queue for real-time processing
        await self.event_queue.publish(MarketDataEvent(transformed_data))
