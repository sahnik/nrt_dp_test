"""Data standardization task component."""

import re
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import pandas as pd
import logging

import sys
sys.path.append('../..')
from dag_orchestrator.task import Task
from dag_orchestrator.context import PipelineContext
from config.logging_config import get_logger


class DataStandardizationTask(Task):
    """
    Task for standardizing data formats, field names, and values.
    """

    def __init__(
        self,
        name: str = "data_standardization",
        field_mappings: Optional[Dict[str, str]] = None,
        type_conversions: Optional[Dict[str, type]] = None,
        default_values: Optional[Dict[str, Any]] = None,
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the data standardization task.
        
        Args:
            name: Task name
            field_mappings: Dictionary mapping old field names to new ones
            type_conversions: Dictionary specifying target types for fields
            default_values: Default values for missing fields
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.field_mappings = field_mappings or {}
        self.type_conversions = type_conversions or {}
        self.default_values = default_values or {}
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the standardization task.
        
        Args:
            context: Pipeline context containing data
            
        Returns:
            Updated context with standardized data
        """
        # Get data from context
        data = context.get("data")
        
        if data is None:
            self.logger.warning("No data found in context")
            return context
        
        # Determine data type and standardize accordingly
        if isinstance(data, pd.DataFrame):
            standardized_data = self._standardize_dataframe(data)
        elif isinstance(data, list):
            standardized_data = self._standardize_list(data)
        elif isinstance(data, dict):
            standardized_data = self._standardize_dict(data)
        else:
            self.logger.warning(f"Unsupported data type: {type(data)}")
            return context
        
        # Update context with standardized data
        context.set("data", standardized_data)
        context.add_task_output(self.name, {
            "records_processed": len(standardized_data) if isinstance(standardized_data, (list, pd.DataFrame)) else 1,
            "fields_mapped": len(self.field_mappings),
            "type_conversions": len(self.type_conversions)
        })
        
        self.logger.info(f"Data standardization complete")
        return context

    def _standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize a pandas DataFrame.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            Standardized DataFrame
        """
        df = df.copy()
        
        # Apply field mappings (rename columns)
        if self.field_mappings:
            df = df.rename(columns=self.field_mappings)
        
        # Apply default values for missing columns
        for field, default_value in self.default_values.items():
            if field not in df.columns:
                df[field] = default_value
        
        # Apply type conversions
        for field, target_type in self.type_conversions.items():
            if field in df.columns:
                df[field] = self._convert_column_type(df[field], target_type)
        
        # Standardize specific fields
        df = self._apply_standard_transformations(df)
        
        return df

    def _standardize_list(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Standardize a list of dictionaries.
        
        Args:
            data_list: List of records to standardize
            
        Returns:
            List of standardized records
        """
        standardized_list = []
        
        for record in data_list:
            if isinstance(record, dict):
                standardized_record = self._standardize_dict(record)
                standardized_list.append(standardized_record)
            else:
                standardized_list.append(record)
        
        return standardized_list

    def _standardize_dict(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Standardize a single dictionary record.
        
        Args:
            record: Record to standardize
            
        Returns:
            Standardized record
        """
        standardized = {}
        
        # Apply field mappings
        for old_key, value in record.items():
            new_key = self.field_mappings.get(old_key, old_key)
            standardized[new_key] = value
        
        # Apply default values for missing fields
        for field, default_value in self.default_values.items():
            if field not in standardized:
                standardized[field] = default_value
        
        # Apply type conversions
        for field, target_type in self.type_conversions.items():
            if field in standardized:
                standardized[field] = self._convert_type(standardized[field], target_type)
        
        # Apply standard transformations
        standardized = self._apply_standard_transformations_dict(standardized)
        
        return standardized

    def _convert_column_type(self, series: pd.Series, target_type: type) -> pd.Series:
        """
        Convert a pandas Series to the target type.
        
        Args:
            series: Series to convert
            target_type: Target type
            
        Returns:
            Converted Series
        """
        try:
            if target_type == str:
                return series.astype(str)
            elif target_type == int:
                return pd.to_numeric(series, errors='coerce').fillna(0).astype(int)
            elif target_type == float:
                return pd.to_numeric(series, errors='coerce')
            elif target_type == bool:
                return series.astype(bool)
            elif target_type == datetime:
                return pd.to_datetime(series, errors='coerce')
            else:
                return series
        except Exception as e:
            self.logger.warning(f"Failed to convert column type: {e}")
            return series

    def _convert_type(self, value: Any, target_type: type) -> Any:
        """
        Convert a value to the target type.
        
        Args:
            value: Value to convert
            target_type: Target type
            
        Returns:
            Converted value
        """
        try:
            if value is None:
                return None
            elif target_type == str:
                return str(value)
            elif target_type == int:
                return int(float(value))
            elif target_type == float:
                return float(value)
            elif target_type == bool:
                return bool(value)
            elif target_type == datetime:
                if isinstance(value, str):
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                return datetime(value)
            else:
                return value
        except Exception as e:
            self.logger.warning(f"Failed to convert value {value} to {target_type}: {e}")
            return value

    def _apply_standard_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply standard transformations to DataFrame.
        
        Args:
            df: DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        # Standardize string columns
        for col in df.select_dtypes(include=['object']).columns:
            # Trim whitespace
            df[col] = df[col].str.strip()
            
            # Standardize case for specific columns
            if col.lower() in ['email', 'user_email']:
                df[col] = df[col].str.lower()
            elif col.lower() in ['country', 'country_code']:
                df[col] = df[col].str.upper()
        
        # Standardize datetime columns
        for col in df.select_dtypes(include=['datetime64']).columns:
            # Ensure UTC timezone
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
        
        return df

    def _apply_standard_transformations_dict(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply standard transformations to a dictionary record.
        
        Args:
            record: Record to transform
            
        Returns:
            Transformed record
        """
        for key, value in record.items():
            if isinstance(value, str):
                # Trim whitespace
                value = value.strip()
                
                # Standardize case for specific fields
                if key.lower() in ['email', 'user_email']:
                    value = value.lower()
                elif key.lower() in ['country', 'country_code']:
                    value = value.upper()
                
                record[key] = value
        
        return record


class FieldMappingTask(Task):
    """
    Task specifically for field mapping and renaming.
    """

    def __init__(
        self,
        name: str = "field_mapping",
        mappings: Dict[str, str] = None,
        remove_unmapped: bool = False,
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the field mapping task.
        
        Args:
            name: Task name
            mappings: Field mappings dictionary
            remove_unmapped: Whether to remove fields not in mappings
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.mappings = mappings or {}
        self.remove_unmapped = remove_unmapped
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the field mapping task.
        
        Args:
            context: Pipeline context
            
        Returns:
            Updated context
        """
        data = context.get("data")
        
        if isinstance(data, pd.DataFrame):
            # Apply mappings to DataFrame columns
            if self.remove_unmapped:
                # Keep only mapped columns
                data = data[list(self.mappings.keys())].rename(columns=self.mappings)
            else:
                data = data.rename(columns=self.mappings)
        
        elif isinstance(data, list):
            # Apply mappings to list of dictionaries
            mapped_data = []
            for record in data:
                if isinstance(record, dict):
                    if self.remove_unmapped:
                        mapped_record = {
                            self.mappings[k]: v 
                            for k, v in record.items() 
                            if k in self.mappings
                        }
                    else:
                        mapped_record = {
                            self.mappings.get(k, k): v 
                            for k, v in record.items()
                        }
                    mapped_data.append(mapped_record)
                else:
                    mapped_data.append(record)
            data = mapped_data
        
        elif isinstance(data, dict):
            # Apply mappings to single dictionary
            if self.remove_unmapped:
                data = {
                    self.mappings[k]: v 
                    for k, v in data.items() 
                    if k in self.mappings
                }
            else:
                data = {
                    self.mappings.get(k, k): v 
                    for k, v in data.items()
                }
        
        context.set("data", data)
        context.add_task_output(self.name, {
            "fields_mapped": len(self.mappings),
            "remove_unmapped": self.remove_unmapped
        })
        
        return context


class DataTypeConversionTask(Task):
    """
    Task for converting data types.
    """

    def __init__(
        self,
        name: str = "type_conversion",
        conversions: Dict[str, str] = None,
        date_format: str = None,
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the type conversion task.
        
        Args:
            name: Task name
            conversions: Dictionary of field -> target_type mappings
            date_format: Date format string for parsing dates
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.conversions = conversions or {}
        self.date_format = date_format
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the type conversion task.
        
        Args:
            context: Pipeline context
            
        Returns:
            Updated context
        """
        data = context.get("data")
        conversion_errors = []
        
        if isinstance(data, pd.DataFrame):
            for field, target_type in self.conversions.items():
                if field in data.columns:
                    try:
                        data[field] = self._convert_series(data[field], target_type)
                    except Exception as e:
                        conversion_errors.append(f"{field}: {e}")
        
        elif isinstance(data, (list, dict)):
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
            
            for field, target_type in self.conversions.items():
                if field in df.columns:
                    try:
                        df[field] = self._convert_series(df[field], target_type)
                    except Exception as e:
                        conversion_errors.append(f"{field}: {e}")
            
            # Convert back to original format
            if isinstance(data, list):
                data = df.to_dict('records')
            else:
                data = df.iloc[0].to_dict()
        
        if conversion_errors:
            context.add_warning(self.name, f"Conversion errors: {', '.join(conversion_errors)}")
        
        context.set("data", data)
        context.add_task_output(self.name, {
            "conversions_attempted": len(self.conversions),
            "conversion_errors": len(conversion_errors)
        })
        
        return context

    def _convert_series(self, series: pd.Series, target_type: str) -> pd.Series:
        """
        Convert a pandas Series to the target type.
        
        Args:
            series: Series to convert
            target_type: Target type as string
            
        Returns:
            Converted Series
        """
        if target_type == "int":
            numeric = pd.to_numeric(series, errors='coerce')
            # Check for conversion failures (NaN values where original wasn't NaN)
            failed_mask = numeric.isna() & ~series.isna()
            if failed_mask.any():
                failed_values = series[failed_mask].tolist()
                raise ValueError(f"Could not convert to int: {failed_values}")
            return numeric.fillna(0).astype(int)
        elif target_type == "float":
            numeric = pd.to_numeric(series, errors='coerce')
            # Check for conversion failures
            failed_mask = numeric.isna() & ~series.isna()
            if failed_mask.any():
                failed_values = series[failed_mask].tolist()
                raise ValueError(f"Could not convert to float: {failed_values}")
            return numeric
        elif target_type == "str":
            return series.astype(str)
        elif target_type == "bool":
            return series.astype(bool)
        elif target_type == "datetime":
            converted = pd.to_datetime(series, format=self.date_format, errors='coerce')
            # Check for conversion failures
            failed_mask = converted.isna() & ~series.isna()
            if failed_mask.any():
                failed_values = series[failed_mask].tolist()
                raise ValueError(f"Could not convert to datetime: {failed_values}")
            return converted
        else:
            raise ValueError(f"Unsupported target type: {target_type}")