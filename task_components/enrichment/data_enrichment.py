"""Data Enrichment Task for lookup-based data enhancement.

This module provides tasks for enriching pipeline data with reference information
from external sources. The enrichment is performed using fast in-memory lookups
from cached reference data.

Key Features:
- Fast O(1) lookups using cached reference data
- Multiple enrichment strategies (replace, append, conditional)
- Configurable lookup miss handling
- Support for multi-field lookups and joins
- Comprehensive metrics and error tracking
- Fallback strategies for missing reference data

Architecture:
    Input Records + Lookup Cache → Enriched Records
    
The enrichment task takes the input records from the pipeline context,
performs lookups against the cached reference data, and produces enriched
records with additional fields from the reference data.

Usage in DAG:
    enrichment = DataEnrichmentTask(
        name="enrichment",
        enrichment_rules=[
            {
                "lookup_key": "customer_id",
                "reference_type": "customers", 
                "target_fields": ["customer_name", "segment"],
                "strategy": "append"
            }
        ]
    )
"""

import asyncio
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from dag_orchestrator.task import Task
from dag_orchestrator.context import PipelineContext
from task_components.enrichment.lookup_cache import LookupCache
from config.logging_config import get_logger


class EnrichmentRule:
    """
    Configuration for a single enrichment operation.
    
    Each rule defines how to lookup reference data and how to merge it
    with the input records. Rules support various strategies for handling
    lookups, missing data, and field conflicts.
    
    Attributes:
        lookup_key: Field name in input record to use for lookup
        reference_type: Type of reference data (customers, products, etc.)
        target_fields: Fields from reference data to include in output
        strategy: How to merge reference data ("append", "replace", "conditional")
        fallback_values: Values to use when lookup fails
        required: Whether enrichment failure should fail the entire record
    """
    
    def __init__(
        self,
        lookup_key: str,
        reference_type: str,
        target_fields: List[str] = None,
        strategy: str = "append",
        fallback_values: Dict[str, Any] = None,
        required: bool = False,
        condition: Optional[str] = None
    ):
        """
        Initialize enrichment rule.
        
        Args:
            lookup_key: Field name in input record to use for lookup
            reference_type: Type of reference data to lookup
            target_fields: Fields from reference data to include (None = all)
            strategy: Merge strategy ("append", "replace", "conditional")
            fallback_values: Default values when lookup fails
            required: Whether lookup failure should fail the record
            condition: Optional condition for when to apply this rule
        """
        self.lookup_key = lookup_key
        self.reference_type = reference_type
        self.target_fields = target_fields or []
        self.strategy = strategy
        self.fallback_values = fallback_values or {}
        self.required = required
        self.condition = condition
        
        # Validation
        if strategy not in ["append", "replace", "conditional"]:
            raise ValueError(f"Invalid strategy: {strategy}")
            
    def should_apply(self, record: Dict[str, Any]) -> bool:
        """
        Check if this rule should be applied to the record.
        
        Args:
            record: Input record to check
            
        Returns:
            True if rule should be applied
        """
        # Check if lookup key exists
        if self.lookup_key not in record:
            return False
            
        # Check if lookup value is valid
        lookup_value = record[self.lookup_key]
        if lookup_value is None or str(lookup_value).strip() == "":
            return False
            
        # Check optional condition
        if self.condition:
            # Simple condition evaluation (can be enhanced)
            # For now, support basic field existence checks
            if self.condition.startswith("exists:"):
                field_name = self.condition[7:]  # Remove "exists:" prefix
                return field_name in record
            elif self.condition.startswith("equals:"):
                # Format: "equals:field_name:value"
                parts = self.condition.split(":", 2)
                if len(parts) == 3:
                    field_name, expected_value = parts[1], parts[2]
                    return str(record.get(field_name)) == expected_value
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert rule to dictionary representation."""
        return {
            "lookup_key": self.lookup_key,
            "reference_type": self.reference_type,
            "target_fields": self.target_fields,
            "strategy": self.strategy,
            "fallback_values": self.fallback_values,
            "required": self.required,
            "condition": self.condition
        }


class DataEnrichmentTask(Task):
    """
    DAG task for enriching records with reference data lookups.
    
    This task performs data enrichment by looking up reference information
    from cached data sources and merging it with the input records. It supports
    multiple enrichment strategies, fallback handling, and comprehensive
    error tracking.
    
    Key Features:
    1. Configure enrichment rules for different lookup types
    2. Support for multiple merge strategies (append, replace, conditional)
    3. Graceful handling of lookup misses with fallback values
    4. Performance metrics and monitoring
    5. Batch processing optimization
    6. Support for both DataFrame and record list inputs
    
    The task expects to find a 'lookup_cache' in the pipeline context, which
    should be provided by a LookupCacheTask upstream in the DAG.
    
    Enrichment Process:
    1. Get input data from pipeline context
    2. Get lookup cache from pipeline context
    3. For each record, apply configured enrichment rules
    4. Perform lookups and merge reference data
    5. Handle lookup misses with fallback strategies  
    6. Output enriched records to pipeline context
    """
    
    def __init__(
        self,
        name: str = "data_enrichment",
        enrichment_rules: List[Union[Dict[str, Any], EnrichmentRule]] = None,
        input_key: str = "data",
        output_key: str = "enriched_data",
        fail_on_cache_miss: bool = False,
        max_lookup_failures: float = 0.2,  # 20% failure threshold
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the data enrichment task.
        
        Args:
            name: Task name
            enrichment_rules: List of enrichment rules to apply
            input_key: Key in context to get input data
            output_key: Key in context to store enriched data
            fail_on_cache_miss: Whether to fail if lookup cache is missing
            max_lookup_failures: Maximum ratio of lookup failures allowed
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        
        self.input_key = input_key
        self.output_key = output_key
        self.fail_on_cache_miss = fail_on_cache_miss
        self.max_lookup_failures = max_lookup_failures
        self.logger = get_logger(f"task.{name}")
        
        # Process enrichment rules
        self.enrichment_rules = []
        for rule in (enrichment_rules or []):
            if isinstance(rule, dict):
                self.enrichment_rules.append(EnrichmentRule(**rule))
            elif isinstance(rule, EnrichmentRule):
                self.enrichment_rules.append(rule)
            else:
                raise ValueError(f"Invalid enrichment rule type: {type(rule)}")
        
        # Metrics
        self.metrics = {
            "records_processed": 0,
            "records_enriched": 0,
            "lookup_attempts": 0,
            "lookup_hits": 0,
            "lookup_misses": 0,
            "enrichment_failures": 0,
            "rules_applied": defaultdict(int)
        }
    
    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the data enrichment task.
        
        Args:
            context: Pipeline context containing input data and lookup cache
            
        Returns:
            Updated context with enriched data
        """
        try:
            # Get input data
            input_data = context.get(self.input_key)
            if input_data is None:
                self.logger.warning(f"No input data found at key: {self.input_key}")
                return context
            
            # Get lookup cache
            lookup_cache = context.get("lookup_cache")
            if lookup_cache is None:
                error_msg = "Lookup cache not found in context"
                if self.fail_on_cache_miss:
                    context.add_error(self.name, error_msg)
                    raise RuntimeError(error_msg)
                else:
                    self.logger.warning(f"{error_msg}, skipping enrichment")
                    context.set(self.output_key, input_data)  # Pass through unchanged
                    return context
            
            # Convert input data to consistent format
            if isinstance(input_data, pd.DataFrame):
                records = input_data.to_dict('records')
                input_format = 'dataframe'
            elif isinstance(input_data, list):
                records = input_data
                input_format = 'list'
            elif isinstance(input_data, dict):
                records = [input_data]
                input_format = 'dict'
            else:
                raise ValueError(f"Unsupported input data type: {type(input_data)}")
            
            # Perform enrichment
            enriched_records = await self._enrich_records(records, lookup_cache)
            
            # Convert back to original format
            if input_format == 'dataframe':
                output_data = pd.DataFrame(enriched_records)
            elif input_format == 'dict':
                output_data = enriched_records[0] if enriched_records else {}
            else:
                output_data = enriched_records
            
            # Store enriched data in context
            context.set(self.output_key, output_data)
            
            # Check failure threshold
            failure_rate = (self.metrics["enrichment_failures"] / 
                          max(self.metrics["records_processed"], 1))
            
            if failure_rate > self.max_lookup_failures:
                error_msg = (f"Enrichment failure rate {failure_rate:.2%} exceeds "
                           f"threshold {self.max_lookup_failures:.2%}")
                context.add_warning(self.name, error_msg)
            
            # Add task output with metrics
            context.add_task_output(self.name, {
                "metrics": dict(self.metrics),
                "failure_rate": failure_rate,
                "enrichment_rules_count": len(self.enrichment_rules),
                "cache_available": lookup_cache is not None
            })
            
            self.logger.info(
                f"Enrichment completed - Processed: {self.metrics['records_processed']}, "
                f"Enriched: {self.metrics['records_enriched']}, "
                f"Lookup hit rate: {self._calculate_hit_rate():.1%}"
            )
            
            return context
            
        except Exception as e:
            self.logger.error(f"Data enrichment failed: {e}")
            context.add_error(self.name, str(e))
            raise
    
    async def _enrich_records(
        self, 
        records: List[Dict[str, Any]], 
        lookup_cache: LookupCache
    ) -> List[Dict[str, Any]]:
        """
        Enrich a list of records using the lookup cache.
        
        Args:
            records: List of input records to enrich
            lookup_cache: Cache containing reference data
            
        Returns:
            List of enriched records
        """
        enriched_records = []
        
        for record in records:
            try:
                enriched_record = await self._enrich_single_record(record, lookup_cache)
                enriched_records.append(enriched_record)
                self.metrics["records_processed"] += 1
                
                # Check if any enrichment was performed
                if self._record_was_enriched(record, enriched_record):
                    self.metrics["records_enriched"] += 1
                    
            except Exception as e:
                self.logger.warning(f"Failed to enrich record: {e}")
                self.metrics["enrichment_failures"] += 1
                
                # Include original record even if enrichment failed
                enriched_records.append(record.copy())
                self.metrics["records_processed"] += 1
        
        return enriched_records
    
    async def _enrich_single_record(
        self, 
        record: Dict[str, Any], 
        lookup_cache: LookupCache
    ) -> Dict[str, Any]:
        """
        Enrich a single record by applying all enrichment rules.
        
        Args:
            record: Input record to enrich
            lookup_cache: Cache containing reference data
            
        Returns:
            Enriched record
        """
        enriched_record = record.copy()
        
        # Apply each enrichment rule
        for rule in self.enrichment_rules:
            try:
                # Check if rule should be applied
                if not rule.should_apply(enriched_record):
                    continue
                
                # Perform lookup
                lookup_value = str(enriched_record[rule.lookup_key])
                cache_key = f"{rule.reference_type}_{lookup_value}"
                
                self.metrics["lookup_attempts"] += 1
                
                # Try cache lookup
                reference_data = lookup_cache.get(cache_key)
                
                if reference_data is not None:
                    # Successful lookup
                    self.metrics["lookup_hits"] += 1
                    self.metrics["rules_applied"][rule.reference_type] += 1
                    
                    # Apply enrichment based on strategy
                    enriched_record = self._apply_enrichment(
                        enriched_record, reference_data, rule
                    )
                    
                else:
                    # Lookup miss - apply fallback strategy
                    self.metrics["lookup_misses"] += 1
                    enriched_record = self._handle_lookup_miss(enriched_record, rule)
                    
            except Exception as e:
                self.logger.warning(
                    f"Rule {rule.reference_type} failed for record: {e}"
                )
                if rule.required:
                    raise  # Re-raise if this rule is required
        
        return enriched_record
    
    def _apply_enrichment(
        self, 
        record: Dict[str, Any], 
        reference_data: Dict[str, Any], 
        rule: EnrichmentRule
    ) -> Dict[str, Any]:
        """
        Apply enrichment data to record based on rule strategy.
        
        Args:
            record: Record to enrich
            reference_data: Reference data from lookup
            rule: Enrichment rule configuration
            
        Returns:
            Enriched record
        """
        enriched_record = record.copy()
        
        # Extract the actual reference data
        ref_fields = reference_data.get("data", {}) if "data" in reference_data else reference_data
        
        # Determine fields to copy
        if rule.target_fields:
            # Only copy specified fields
            fields_to_copy = {
                field: ref_fields.get(field) 
                for field in rule.target_fields 
                if field in ref_fields
            }
        else:
            # Copy all fields except metadata
            fields_to_copy = {
                k: v for k, v in ref_fields.items() 
                if not k.startswith('_')  # Skip metadata fields
            }
        
        # Apply strategy
        if rule.strategy == "append":
            # Add new fields, don't overwrite existing
            for field, value in fields_to_copy.items():
                if field not in enriched_record:
                    enriched_record[field] = value
                    
        elif rule.strategy == "replace":
            # Overwrite existing fields
            enriched_record.update(fields_to_copy)
            
        elif rule.strategy == "conditional":
            # Only add if target fields are empty/missing
            for field, value in fields_to_copy.items():
                existing_value = enriched_record.get(field)
                if existing_value is None or str(existing_value).strip() == "":
                    enriched_record[field] = value
        
        # Add enrichment metadata
        enriched_record[f"_{rule.reference_type}_enriched"] = True
        enriched_record[f"_{rule.reference_type}_enriched_at"] = datetime.utcnow().isoformat()
        
        return enriched_record
    
    def _handle_lookup_miss(
        self, 
        record: Dict[str, Any], 
        rule: EnrichmentRule
    ) -> Dict[str, Any]:
        """
        Handle lookup miss with fallback strategies.
        
        Args:
            record: Record being processed
            rule: Enrichment rule that failed
            
        Returns:
            Record with fallback handling applied
        """
        enriched_record = record.copy()
        
        # Apply fallback values if configured
        if rule.fallback_values:
            for field, value in rule.fallback_values.items():
                if rule.strategy == "append" and field in enriched_record:
                    continue  # Don't overwrite existing values in append mode
                enriched_record[field] = value
        
        # Add metadata about the lookup miss
        enriched_record[f"_{rule.reference_type}_lookup_status"] = "miss"
        enriched_record[f"_{rule.reference_type}_lookup_key"] = record.get(rule.lookup_key)
        
        # Mark for potential retry/reprocessing
        enriched_record["_enrichment_incomplete"] = True
        
        return enriched_record
    
    def _record_was_enriched(
        self, 
        original: Dict[str, Any], 
        enriched: Dict[str, Any]
    ) -> bool:
        """
        Check if a record was actually enriched.
        
        Args:
            original: Original record
            enriched: Potentially enriched record
            
        Returns:
            True if record was enriched
        """
        return len(enriched) > len(original)
    
    def _calculate_hit_rate(self) -> float:
        """Calculate lookup hit rate."""
        total_lookups = self.metrics["lookup_attempts"]
        if total_lookups == 0:
            return 0.0
        return self.metrics["lookup_hits"] / total_lookups
    
    def get_enrichment_stats(self) -> Dict[str, Any]:
        """Get detailed enrichment statistics."""
        return {
            "task_name": self.name,
            "enrichment_rules": len(self.enrichment_rules),
            "metrics": dict(self.metrics),
            "hit_rate": self._calculate_hit_rate(),
            "rules_config": [rule.to_dict() for rule in self.enrichment_rules]
        }


# Import defaultdict for metrics
from collections import defaultdict