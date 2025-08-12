"""Tests for standardization task components."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

import sys
sys.path.append('..')

from dag_orchestrator.context import PipelineContext
from task_components.standardization.standardizer import (
    DataStandardizationTask,
    FieldMappingTask,
    DataTypeConversionTask
)


class TestDataStandardizationTask:
    """Test DataStandardizationTask functionality."""
    
    @pytest.mark.asyncio
    async def test_dataframe_standardization(self):
        """Test standardizing a pandas DataFrame."""
        # Create test data
        test_data = pd.DataFrame([
            {
                "user_id": "user_001",
                "product_id": "prod_001",
                "event_type": "purchase",
                "price": "99.99",
                "quantity": "2",
                "timestamp": "2024-01-01T10:00:00Z",
                "email": " USER@EXAMPLE.COM ",
                "country": "us"
            },
            {
                "user_id": "user_002",
                "product_id": "prod_002",
                "event_type": "view",
                "price": "149.50",
                "quantity": "1",
                "timestamp": "2024-01-01T11:00:00Z",
                "email": "user2@example.com",
                "country": "uk"
            }
        ])
        
        # Create standardization task
        task = DataStandardizationTask(
            field_mappings={
                "user_id": "userId",
                "product_id": "productId",
                "event_type": "eventType"
            },
            type_conversions={
                "price": float,
                "quantity": int,
                "timestamp": datetime
            },
            default_values={
                "processed_at": "2024-01-01T12:00:00Z"
            }
        )
        
        # Execute task
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        # Verify results
        result_data = result_context.get("data")
        assert isinstance(result_data, pd.DataFrame)
        assert len(result_data) == 2
        
        # Check field mappings
        assert "userId" in result_data.columns
        assert "productId" in result_data.columns
        assert "eventType" in result_data.columns
        
        # Check type conversions
        assert result_data["price"].dtype in [float, 'float64']
        assert result_data["quantity"].dtype in [int, 'int64']
        
        # Check default values
        assert "processed_at" in result_data.columns
        assert all(result_data["processed_at"] == "2024-01-01T12:00:00Z")
        
        # Check string standardization
        assert all(result_data["email"].str.strip() == result_data["email"])
        assert result_data.iloc[0]["email"] == "user@example.com"
        assert result_data.iloc[0]["country"] == "US"
        assert result_data.iloc[1]["country"] == "UK"
    
    @pytest.mark.asyncio
    async def test_list_standardization(self):
        """Test standardizing a list of dictionaries."""
        test_data = [
            {
                "user_id": "user_001",
                "price": "99.99",
                "email": " USER@EXAMPLE.COM ",
                "country": "us"
            },
            {
                "user_id": "user_002",
                "price": "149.50",
                "email": "user2@example.com",
                "country": "uk"
            }
        ]
        
        task = DataStandardizationTask(
            field_mappings={"user_id": "userId"},
            type_conversions={"price": float},
            default_values={"processed": True}
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        assert isinstance(result_data, list)
        assert len(result_data) == 2
        
        # Check transformations
        for record in result_data:
            assert "userId" in record
            assert "user_id" not in record
            assert isinstance(record["price"], float)
            assert record["processed"] is True
            assert record["email"].strip() == record["email"]
    
    @pytest.mark.asyncio
    async def test_single_dict_standardization(self):
        """Test standardizing a single dictionary."""
        test_data = {
            "user_id": "user_001",
            "price": "99.99",
            "email": " USER@EXAMPLE.COM "
        }
        
        task = DataStandardizationTask(
            field_mappings={"user_id": "userId"},
            type_conversions={"price": float}
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        assert isinstance(result_data, dict)
        assert "userId" in result_data
        assert isinstance(result_data["price"], float)
        assert result_data["email"] == "user@example.com"


class TestFieldMappingTask:
    """Test FieldMappingTask functionality."""
    
    @pytest.mark.asyncio
    async def test_field_mapping_dataframe(self):
        """Test field mapping on DataFrame."""
        test_data = pd.DataFrame([
            {"old_field1": "value1", "old_field2": "value2", "keep_field": "keep"},
            {"old_field1": "value3", "old_field2": "value4", "keep_field": "keep"}
        ])
        
        task = FieldMappingTask(
            mappings={
                "old_field1": "new_field1",
                "old_field2": "new_field2"
            },
            remove_unmapped=False
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        
        # Check mapped fields
        assert "new_field1" in result_data.columns
        assert "new_field2" in result_data.columns
        assert "old_field1" not in result_data.columns
        assert "old_field2" not in result_data.columns
        
        # Check unmapped field is kept
        assert "keep_field" in result_data.columns
    
    @pytest.mark.asyncio
    async def test_field_mapping_remove_unmapped(self):
        """Test field mapping with removal of unmapped fields."""
        test_data = [
            {"old_field": "value1", "remove_me": "unwanted"},
            {"old_field": "value2", "remove_me": "unwanted"}
        ]
        
        task = FieldMappingTask(
            mappings={"old_field": "new_field"},
            remove_unmapped=True
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        
        for record in result_data:
            assert "new_field" in record
            assert "old_field" not in record
            assert "remove_me" not in record


class TestDataTypeConversionTask:
    """Test DataTypeConversionTask functionality."""
    
    @pytest.mark.asyncio
    async def test_type_conversions(self):
        """Test various type conversions."""
        test_data = pd.DataFrame([
            {
                "int_field": "123",
                "float_field": "45.67",
                "str_field": 789,
                "datetime_field": "2024-01-01T10:00:00"
            }
        ])
        
        task = DataTypeConversionTask(
            conversions={
                "int_field": "int",
                "float_field": "float",
                "str_field": "str",
                "datetime_field": "datetime"
            }
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        
        # Check conversions
        assert result_data.iloc[0]["int_field"] == 123
        assert isinstance(result_data.iloc[0]["int_field"], (int, np.integer))
        
        assert abs(result_data.iloc[0]["float_field"] - 45.67) < 0.001
        assert isinstance(result_data.iloc[0]["float_field"], float)
        
        assert result_data.iloc[0]["str_field"] == "789"
        assert isinstance(result_data.iloc[0]["str_field"], str)
    
    @pytest.mark.asyncio
    async def test_conversion_errors(self):
        """Test handling of conversion errors."""
        test_data = [
            {
                "invalid_int": "not_a_number",
                "invalid_float": "also_not_a_number"
            }
        ]
        
        task = DataTypeConversionTask(
            conversions={
                "invalid_int": "int",
                "invalid_float": "float"
            }
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        # Task should complete with warnings about conversion errors
        task_output = result_context.get_task_output("type_conversion")
        assert task_output["conversion_errors"] > 0
        
        # Check for warnings in context
        warnings = result_context.get_metadata().get("warnings", [])
        assert any("Conversion errors" in str(w) for w in warnings)
    
    @pytest.mark.asyncio
    async def test_datetime_conversion_with_format(self):
        """Test datetime conversion with custom format."""
        test_data = pd.DataFrame([
            {"date_field": "01/15/2024 14:30:00"}
        ])
        
        task = DataTypeConversionTask(
            conversions={"date_field": "datetime"},
            date_format="%m/%d/%Y %H:%M:%S"
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        
        # Check datetime conversion
        date_value = result_data.iloc[0]["date_field"]
        assert pd.isna(date_value) or isinstance(date_value, (pd.Timestamp, datetime))


@pytest.mark.asyncio
class TestIntegratedStandardization:
    """Test integrated standardization workflow."""
    
    async def test_complete_standardization_pipeline(self):
        """Test a complete standardization pipeline."""
        # Original messy data
        test_data = [
            {
                "user_id": "user_001",
                "product_id": "prod_001",
                "event_type": "purchase",
                "price": "99.99",
                "quantity": "2",
                "email": " USER@EXAMPLE.COM ",
                "signup_date": "2024-01-01"
            },
            {
                "user_id": "user_002", 
                "product_id": "prod_002",
                "event_type": "view",
                "price": "149.50",
                "quantity": "1",
                "email": "user2@example.com",
                "signup_date": "2024-01-02"
            }
        ]
        
        # Step 1: Field mapping
        field_mapper = FieldMappingTask(
            mappings={
                "user_id": "userId",
                "product_id": "productId",
                "event_type": "eventType",
                "signup_date": "signupDate"
            }
        )
        
        # Step 2: Type conversion
        type_converter = DataTypeConversionTask(
            conversions={
                "price": "float",
                "quantity": "int",
                "signupDate": "datetime"
            }
        )
        
        # Step 3: Standardization
        standardizer = DataStandardizationTask(
            default_values={
                "processed_at": "2024-01-01T12:00:00Z"
            }
        )
        
        # Execute pipeline
        context = PipelineContext({"data": test_data})
        
        # Field mapping
        context = await field_mapper.execute(context)
        
        # Type conversion  
        context = await type_converter.execute(context)
        
        # Standardization
        context = await standardizer.execute(context)
        
        # Verify final result
        result_data = context.get("data")
        
        assert len(result_data) == 2
        
        for record in result_data:
            # Check field mappings
            assert "userId" in record
            assert "productId" in record
            assert "eventType" in record
            
            # Check type conversions
            assert isinstance(record["price"], float)
            assert isinstance(record["quantity"], int)
            
            # Check standardization
            assert record["processed_at"] == "2024-01-01T12:00:00Z"
            assert record["email"].strip() == record["email"]
            if "USER@EXAMPLE.COM" in record["email"].upper():
                assert record["email"] == "user@example.com"


if __name__ == "__main__":
    pytest.main([__file__])