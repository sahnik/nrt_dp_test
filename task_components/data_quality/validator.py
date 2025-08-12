"""Data quality validation task component."""

from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime
import pandas as pd
import re
import logging
from enum import Enum

import sys
sys.path.append('../..')
from dag_orchestrator.task import Task
from dag_orchestrator.context import PipelineContext
from config.logging_config import get_logger


class ValidationRule:
    """Base class for validation rules."""
    
    def __init__(self, field: str, error_message: str = None):
        """
        Initialize validation rule.
        
        Args:
            field: Field to validate
            error_message: Custom error message
        """
        self.field = field
        self.error_message = error_message or f"Validation failed for field {field}"
    
    def validate(self, value: Any) -> bool:
        """
        Validate a value.
        
        Args:
            value: Value to validate
            
        Returns:
            True if valid, False otherwise
        """
        raise NotImplementedError


class NotNullRule(ValidationRule):
    """Rule to check for null values."""
    
    def validate(self, value: Any) -> bool:
        """Check if value is not null."""
        return value is not None and pd.notna(value)


class RangeRule(ValidationRule):
    """Rule to check if value is within range."""
    
    def __init__(self, field: str, min_value: float = None, max_value: float = None, error_message: str = None):
        """
        Initialize range rule.
        
        Args:
            field: Field to validate
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            error_message: Custom error message
        """
        super().__init__(field, error_message)
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, value: Any) -> bool:
        """Check if value is within range."""
        if pd.isna(value):
            return False
        
        try:
            num_value = float(value)
            if self.min_value is not None and num_value < self.min_value:
                return False
            if self.max_value is not None and num_value > self.max_value:
                return False
            return True
        except (ValueError, TypeError):
            return False


class PatternRule(ValidationRule):
    """Rule to check if value matches a pattern."""
    
    def __init__(self, field: str, pattern: str, error_message: str = None):
        """
        Initialize pattern rule.
        
        Args:
            field: Field to validate
            pattern: Regex pattern to match
            error_message: Custom error message
        """
        super().__init__(field, error_message)
        self.pattern = re.compile(pattern)
    
    def validate(self, value: Any) -> bool:
        """Check if value matches pattern."""
        if pd.isna(value):
            return False
        
        return bool(self.pattern.match(str(value)))


class TypeRule(ValidationRule):
    """Rule to check value type."""
    
    def __init__(self, field: str, expected_type: type, error_message: str = None):
        """
        Initialize type rule.
        
        Args:
            field: Field to validate
            expected_type: Expected type
            error_message: Custom error message
        """
        super().__init__(field, error_message)
        self.expected_type = expected_type
    
    def validate(self, value: Any) -> bool:
        """Check if value is of expected type."""
        if pd.isna(value):
            return False
        
        if self.expected_type == int:
            try:
                # Check if it's already an int
                if isinstance(value, int) and not isinstance(value, bool):
                    return True
                # For strings, try to convert and check if it's a whole number
                if isinstance(value, str):
                    int(value)  # This will raise if not convertible
                    return True
                # For floats, only accept if it's a whole number
                if isinstance(value, float):
                    return False  # Reject floats for strict int validation
                return False
            except:
                return False
        elif self.expected_type == float:
            try:
                float(value)
                return True
            except:
                return False
        elif self.expected_type == str:
            return isinstance(value, str)
        elif self.expected_type == bool:
            return isinstance(value, bool)
        else:
            return isinstance(value, self.expected_type)


class DataQualityTask(Task):
    """
    Task for validating data quality.
    """

    def __init__(
        self,
        name: str = "data_quality_validation",
        rules: List[ValidationRule] = None,
        fail_on_error: bool = False,
        error_threshold: float = 0.1,
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the data quality task.
        
        Args:
            name: Task name
            rules: List of validation rules
            fail_on_error: Whether to fail the task on validation errors
            error_threshold: Maximum allowed error rate (0.0 to 1.0)
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.rules = rules or []
        self.fail_on_error = fail_on_error
        self.error_threshold = error_threshold
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the data quality validation task.
        
        Args:
            context: Pipeline context containing data
            
        Returns:
            Updated context with validation results
        """
        data = context.get("data")
        
        if data is None:
            self.logger.warning("No data found in context")
            return context
        
        # Perform validation based on data type
        if isinstance(data, pd.DataFrame):
            validation_results = self._validate_dataframe(data)
        elif isinstance(data, list):
            validation_results = self._validate_list(data)
        elif isinstance(data, dict):
            validation_results = self._validate_dict(data)
        else:
            self.logger.warning(f"Unsupported data type: {type(data)}")
            return context
        
        # Calculate error rate
        total_checks = validation_results["total_checks"]
        failed_checks = validation_results["failed_checks"]
        error_rate = failed_checks / total_checks if total_checks > 0 else 0
        
        # Add validation results to context
        context.add_task_output(self.name, validation_results)
        
        # Handle validation failures
        if error_rate > self.error_threshold:
            error_msg = f"Data quality validation failed: error rate {error_rate:.2%} exceeds threshold {self.error_threshold:.2%}"
            context.add_error(self.name, error_msg)
            
            if self.fail_on_error:
                raise ValueError(error_msg)
        
        # Add clean data to context (records that passed validation)
        if "clean_data" in validation_results:
            context.set("clean_data", validation_results["clean_data"])
        
        # Add invalid data to context for further analysis
        if "invalid_data" in validation_results:
            context.set("invalid_data", validation_results["invalid_data"])
        
        self.logger.info(f"Data quality validation complete - Error rate: {error_rate:.2%}")
        return context

    def _validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate a pandas DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validation results
        """
        total_records = len(df)
        total_checks = 0
        failed_checks = 0
        errors_by_field = {}
        invalid_indices = set()
        
        for rule in self.rules:
            if rule.field not in df.columns:
                self.logger.warning(f"Field {rule.field} not found in DataFrame")
                continue
            
            # Validate each value in the column
            for idx, value in df[rule.field].items():
                total_checks += 1
                if not rule.validate(value):
                    failed_checks += 1
                    invalid_indices.add(idx)
                    
                    if rule.field not in errors_by_field:
                        errors_by_field[rule.field] = []
                    errors_by_field[rule.field].append({
                        "index": idx,
                        "value": value,
                        "error": rule.error_message
                    })
        
        # Separate clean and invalid data
        clean_data = df[~df.index.isin(invalid_indices)]
        invalid_data = df[df.index.isin(invalid_indices)]
        
        return {
            "total_records": total_records,
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "error_rate": failed_checks / total_checks if total_checks > 0 else 0,
            "errors_by_field": errors_by_field,
            "invalid_record_count": len(invalid_indices),
            "clean_data": clean_data,
            "invalid_data": invalid_data
        }

    def _validate_list(self, data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a list of dictionaries.
        
        Args:
            data_list: List of records to validate
            
        Returns:
            Validation results
        """
        total_records = len(data_list)
        total_checks = 0
        failed_checks = 0
        errors_by_field = {}
        clean_data = []
        invalid_data = []
        
        for idx, record in enumerate(data_list):
            if not isinstance(record, dict):
                invalid_data.append(record)
                continue
            
            record_valid = True
            record_errors = []
            
            for rule in self.rules:
                if rule.field not in record:
                    self.logger.warning(f"Field {rule.field} not found in record {idx}")
                    continue
                
                total_checks += 1
                value = record[rule.field]
                
                if not rule.validate(value):
                    failed_checks += 1
                    record_valid = False
                    record_errors.append({
                        "field": rule.field,
                        "value": value,
                        "error": rule.error_message
                    })
                    
                    if rule.field not in errors_by_field:
                        errors_by_field[rule.field] = []
                    errors_by_field[rule.field].append({
                        "index": idx,
                        "value": value,
                        "error": rule.error_message
                    })
            
            if record_valid:
                clean_data.append(record)
            else:
                invalid_data.append({
                    "record": record,
                    "errors": record_errors
                })
        
        return {
            "total_records": total_records,
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "error_rate": failed_checks / total_checks if total_checks > 0 else 0,
            "errors_by_field": errors_by_field,
            "invalid_record_count": len(invalid_data),
            "clean_data": clean_data,
            "invalid_data": invalid_data
        }

    def _validate_dict(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single dictionary record.
        
        Args:
            record: Record to validate
            
        Returns:
            Validation results
        """
        total_checks = 0
        failed_checks = 0
        errors = []
        
        for rule in self.rules:
            if rule.field not in record:
                self.logger.warning(f"Field {rule.field} not found in record")
                continue
            
            total_checks += 1
            value = record[rule.field]
            
            if not rule.validate(value):
                failed_checks += 1
                errors.append({
                    "field": rule.field,
                    "value": value,
                    "error": rule.error_message
                })
        
        is_valid = failed_checks == 0
        
        return {
            "total_records": 1,
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "error_rate": failed_checks / total_checks if total_checks > 0 else 0,
            "errors": errors,
            "is_valid": is_valid,
            "clean_data": record if is_valid else None,
            "invalid_data": {"record": record, "errors": errors} if not is_valid else None
        }


class DuplicateCheckTask(Task):
    """
    Task for checking and handling duplicate records.
    """

    def __init__(
        self,
        name: str = "duplicate_check",
        key_fields: List[str] = None,
        action: str = "remove",  # remove, mark, or keep
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the duplicate check task.
        
        Args:
            name: Task name
            key_fields: Fields to use for duplicate detection
            action: Action to take on duplicates (remove, mark, or keep)
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.key_fields = key_fields or []
        self.action = action
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the duplicate check task.
        
        Args:
            context: Pipeline context
            
        Returns:
            Updated context
        """
        data = context.get("data")
        
        if isinstance(data, pd.DataFrame):
            result_data, duplicates_info = self._handle_dataframe_duplicates(data)
        elif isinstance(data, list):
            result_data, duplicates_info = self._handle_list_duplicates(data)
        else:
            self.logger.warning(f"Unsupported data type for duplicate check: {type(data)}")
            return context
        
        # Update context
        context.set("data", result_data)
        context.add_task_output(self.name, duplicates_info)
        
        if duplicates_info["duplicate_count"] > 0:
            context.add_warning(
                self.name,
                f"Found {duplicates_info['duplicate_count']} duplicate records"
            )
        
        return context

    def _handle_dataframe_duplicates(self, df: pd.DataFrame) -> tuple:
        """
        Handle duplicates in a DataFrame.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Tuple of (processed DataFrame, duplicates info)
        """
        if not self.key_fields:
            # Use all columns if no key fields specified
            duplicates = df.duplicated()
        else:
            duplicates = df.duplicated(subset=self.key_fields)
        
        duplicate_count = duplicates.sum()
        
        if self.action == "remove":
            result_df = df[~duplicates]
        elif self.action == "mark":
            df["is_duplicate"] = duplicates
            result_df = df
        else:  # keep
            result_df = df
        
        duplicates_info = {
            "duplicate_count": int(duplicate_count),
            "original_count": len(df),
            "final_count": len(result_df),
            "action": self.action,
            "key_fields": self.key_fields
        }
        
        return result_df, duplicates_info

    def _handle_list_duplicates(self, data_list: List[Dict[str, Any]]) -> tuple:
        """
        Handle duplicates in a list of dictionaries.
        
        Args:
            data_list: List to check
            
        Returns:
            Tuple of (processed list, duplicates info)
        """
        seen = set()
        result_list = []
        duplicate_count = 0
        
        for record in data_list:
            if isinstance(record, dict):
                # Create key from specified fields
                if self.key_fields:
                    key = tuple(record.get(field) for field in self.key_fields)
                else:
                    key = tuple(sorted(record.items()))
                
                if key in seen:
                    duplicate_count += 1
                    if self.action == "mark":
                        record["is_duplicate"] = True
                        result_list.append(record)
                    elif self.action == "keep":
                        result_list.append(record)
                    # If action is "remove", don't add to result
                else:
                    seen.add(key)
                    if self.action == "mark":
                        record["is_duplicate"] = False
                    result_list.append(record)
            else:
                result_list.append(record)
        
        duplicates_info = {
            "duplicate_count": duplicate_count,
            "original_count": len(data_list),
            "final_count": len(result_list),
            "action": self.action,
            "key_fields": self.key_fields
        }
        
        return result_list, duplicates_info