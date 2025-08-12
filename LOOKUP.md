This is a classic data enrichment
challenge in streaming architectures. Let me analyze the
options and recommend the best approach for your NRT
pipeline.

Analysis of Requirements

1. Performance: Cannot query Oracle per record (would
destroy NRT performance)
2. Data freshness: Oracle updated nightly, so data is
eventually consistent
3. Lookup misses: Must handle gracefully when referenced
data isn't available yet
4. Scalability: Solution must work with multiple pipeline
instances

Recommended Architecture: Kafka-Based Lookup Cache

I recommend a hybrid Kafka + local cache approach with the
following components:

1. Oracle Data Publisher Service

Oracle Nightly Batch → Data Publisher → Kafka Lookup Topic
- Separate service that publishes Oracle reference data to
Kafka after nightly loads
- Uses compacted Kafka topic (latest value per key)
- Publishes full dataset initially, then incremental updates

2. Lookup Cache Manager Task

- DAG task that maintains in-memory lookup cache
- Consumes from Kafka lookup topic on startup/refresh
- Handles cache warming and updates

3. Data Enrichment Task

- Performs actual lookups against local cache
- Handles lookup misses gracefully
- Enriches records with reference data

Implementation Plan

Component 1: Oracle Data Publisher (Separate Service)

class OracleDataPublisher:
    """Publishes Oracle reference data to Kafka after 
nightly batch"""

    async def publish_reference_data(self):
        # Query Oracle for reference data
        # Publish to compacted Kafka topic: lookup-data
        # Key: lookup_key, Value: reference_data

Component 2: Lookup Cache Manager Task

class LookupCacheTask(Task):
    """Maintains in-memory lookup cache from Kafka"""

    def __init__(self):
        self.cache = {}  # In-memory lookup cache
        self.consumer = None  # Kafka consumer for lookup 
topic

    async def execute(self, context: PipelineContext):
        # Refresh cache if needed (startup, periodic, or 
forced)
        if self._should_refresh_cache():
            await self._refresh_cache()

        # Make cache available to downstream tasks
        context.set("lookup_cache", self.cache)
        return context

    async def _refresh_cache(self):
        # Consume from lookup-data topic
        # Update in-memory cache
        # Handle compacted topic (latest values only)

Component 3: Data Enrichment Task

class DataEnrichmentTask(Task):
    """Enriches records with lookup data"""

    async def execute(self, context: PipelineContext):
        data = context.get("data")  # Input records
        cache = context.get("lookup_cache")  # Lookup cache

        enriched_records = []
        for record in data:
            # Perform lookup
            lookup_key = record.get("lookup_id")
            reference_data = cache.get(lookup_key)

            if reference_data:
                # Enrich record with reference data
                record.update(reference_data)
                record["_enrichment_status"] = "enriched"
            else:
                # Handle lookup miss
                record["_enrichment_status"] = "lookup_miss"
                self._handle_lookup_miss(record)

            enriched_records.append(record)

        context.set("enriched_data", enriched_records)
        return context

Alternative Approaches Considered

Option A: Redis Cache

Pros: Shared across instances, TTL support, persistence
Cons: Network latency, additional infrastructure, single
point of failure

Option B: Local File Cache

Pros: Fastest lookups, no dependencies
Cons: File sync complexity, startup time, disk I/O

Option C: Direct Oracle Connection Pool

Pros: Always current data
Cons: Defeats NRT performance, Oracle load, connection
management

Handling Lookup Misses

Strategy 1: Default Values

def _handle_lookup_miss(self, record):
    record["reference_field"] = "UNKNOWN"
    record["_needs_enrichment"] = True

Strategy 2: Dead Letter Queue

def _handle_lookup_miss(self, record):
    # Send to DLQ for later enrichment
    await self.dlq_producer.send(record)

Strategy 3: Delayed Enrichment

def _handle_lookup_miss(self, record):
    # Mark for background enrichment
    record["_enrichment_retry_count"] = 0
    record["_enrichment_next_attempt"] = time.time() + 3600

DAG Integration

The enrichment would fit into your existing DAG like this:

Field Mapping → Type Conversion → Lookup Cache → Data
Enrichment →
Standardization → Duplicate Check → Quality Validation →
Dual Sinks

Performance Considerations

1. Cache Size: Monitor memory usage, implement cache
eviction if needed
2. Cache Refresh: Implement smart refresh (delta updates,
scheduled refresh)
3. Lookup Performance: Use efficient data structures (dict
lookups = O(1))
4. Pipeline Impact: Ensure cache operations don't block
record processing

Monitoring & Observability

1. Cache Metrics: Hit rate, miss rate, cache size, refresh
time
2. Enrichment Metrics: Success rate, lookup miss percentage
3. Performance Metrics: Lookup latency, cache refresh
duration