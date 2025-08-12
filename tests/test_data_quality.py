"""Tests for data quality validation components."""

import pytest
import pandas as pd

import sys
sys.path.append('..')

from dag_orchestrator.context import PipelineContext
from task_components.data_quality.validator import (
    DataQualityTask,
    DuplicateCheckTask,
    ValidationRule,
    NotNullRule,
    RangeRule,
    PatternRule,
    TypeRule
)


class TestValidationRules:
    """Test individual validation rules."""
    
    def test_not_null_rule(self):
        """Test NotNullRule."""
        rule = NotNullRule("test_field")
        
        assert rule.validate("value") is True
        assert rule.validate(0) is True
        assert rule.validate(False) is True
        assert rule.validate("") is True
        
        assert rule.validate(None) is False
        assert rule.validate(pd.NA) is False
    
    def test_range_rule(self):
        """Test RangeRule."""
        rule = RangeRule("test_field", min_value=0, max_value=100)
        
        assert rule.validate(50) is True
        assert rule.validate(0) is True
        assert rule.validate(100) is True
        assert rule.validate("50") is True
        
        assert rule.validate(-1) is False
        assert rule.validate(101) is False
        assert rule.validate("not_a_number") is False
        assert rule.validate(None) is False
    
    def test_pattern_rule(self):
        """Test PatternRule."""
        email_rule = PatternRule("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        
        assert email_rule.validate("user@example.com") is True
        assert email_rule.validate("test.user+tag@domain.co.uk") is True
        
        assert email_rule.validate("invalid-email") is False
        assert email_rule.validate("@domain.com") is False
        assert email_rule.validate("user@") is False
        assert email_rule.validate(None) is False
    
    def test_type_rule(self):
        """Test TypeRule."""
        int_rule = TypeRule("test_field", int)
        
        assert int_rule.validate(123) is True
        assert int_rule.validate("456") is True
        assert int_rule.validate("0") is True
        
        assert int_rule.validate("not_a_number") is False
        assert int_rule.validate(12.34) is False
        assert int_rule.validate(None) is False


class TestDataQualityTask:
    """Test DataQualityTask functionality."""
    
    @pytest.mark.asyncio
    async def test_dataframe_validation(self):
        """Test validating a pandas DataFrame."""
        test_data = pd.DataFrame([
            {
                "id": "1",
                "email": "user1@example.com",
                "age": 25,
                "score": 85.5
            },
            {
                "id": "2", 
                "email": "user2@example.com",
                "age": 30,
                "score": 92.0
            },
            {
                "id": None,  # Invalid - null ID
                "email": "invalid-email",  # Invalid email
                "age": -5,  # Invalid age
                "score": 150  # Invalid score (out of range)
            }
        ])
        
        rules = [
            NotNullRule("id"),
            PatternRule("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
            RangeRule("age", min_value=0, max_value=120),
            RangeRule("score", min_value=0, max_value=100)
        ]
        
        task = DataQualityTask(
            rules=rules,
            fail_on_error=False,
            error_threshold=0.5
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        # Check task output
        task_output = result_context.get_task_output("data_quality_validation")
        assert task_output["total_records"] == 3
        assert task_output["total_checks"] == 12  # 4 rules × 3 records
        assert task_output["failed_checks"] == 4  # All 4 rules fail for third record
        assert task_output["error_rate"] == 4/12
        
        # Check clean and invalid data separation
        clean_data = result_context.get("clean_data")
        invalid_data = result_context.get("invalid_data")
        
        assert len(clean_data) == 2  # First two records are valid
        assert len(invalid_data) == 1  # Third record is invalid
    
    @pytest.mark.asyncio
    async def test_list_validation(self):
        """Test validating a list of dictionaries."""
        test_data = [
            {"id": "1", "score": 85},
            {"id": "2", "score": 92},
            {"id": None, "score": 150}  # Invalid record
        ]
        
        rules = [
            NotNullRule("id"),
            RangeRule("score", min_value=0, max_value=100)
        ]
        
        task = DataQualityTask(rules=rules)
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        clean_data = result_context.get("clean_data")
        invalid_data = result_context.get("invalid_data")
        
        assert len(clean_data) == 2
        assert len(invalid_data) == 1
        assert invalid_data[0]["record"]["id"] is None
    
    @pytest.mark.asyncio
    async def test_validation_threshold_failure(self):
        """Test failure when error threshold is exceeded."""
        test_data = [
            {"id": None},  # Invalid
            {"id": None},  # Invalid
            {"id": "3"}    # Valid
        ]
        
        rules = [NotNullRule("id")]
        
        task = DataQualityTask(
            rules=rules,
            fail_on_error=True,
            error_threshold=0.5  # 50% threshold, but we have 67% errors
        )
        
        context = PipelineContext({"data": test_data})
        
        with pytest.raises(ValueError, match="error rate.*exceeds threshold"):
            await task.execute(context)
    
    @pytest.mark.asyncio
    async def test_single_dict_validation(self):
        """Test validating a single dictionary."""
        test_data = {"id": "1", "email": "user@example.com", "age": 25}
        
        rules = [
            NotNullRule("id"),
            PatternRule("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
            RangeRule("age", min_value=0, max_value=120)
        ]
        
        task = DataQualityTask(rules=rules)
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        task_output = result_context.get_task_output("data_quality_validation")
        assert task_output["is_valid"] is True
        assert task_output["failed_checks"] == 0


class TestDuplicateCheckTask:
    """Test DuplicateCheckTask functionality."""
    
    @pytest.mark.asyncio
    async def test_dataframe_duplicate_removal(self):
        """Test removing duplicates from DataFrame."""
        test_data = pd.DataFrame([
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob", "email": "bob@example.com"},
            {"id": "1", "name": "Alice", "email": "alice@example.com"},  # Exact duplicate
            {"id": "3", "name": "Charlie", "email": "charlie@example.com"}
        ])
        
        task = DuplicateCheckTask(
            key_fields=["id", "email"],
            action="remove"
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        task_output = result_context.get_task_output("duplicate_check")
        
        assert len(result_data) == 3  # One duplicate removed
        assert task_output["duplicate_count"] == 1
        assert task_output["original_count"] == 4
        assert task_output["final_count"] == 3
    
    @pytest.mark.asyncio
    async def test_list_duplicate_marking(self):
        """Test marking duplicates in a list."""
        test_data = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "1", "name": "Alice"},  # Duplicate
            {"id": "3", "name": "Charlie"}
        ]
        
        task = DuplicateCheckTask(
            key_fields=["id"],
            action="mark"
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        
        assert len(result_data) == 4  # All records kept
        assert result_data[0]["is_duplicate"] is False
        assert result_data[1]["is_duplicate"] is False
        assert result_data[2]["is_duplicate"] is True  # Marked as duplicate
        assert result_data[3]["is_duplicate"] is False
    
    @pytest.mark.asyncio
    async def test_duplicate_keep_all(self):
        """Test keeping all records including duplicates."""
        test_data = pd.DataFrame([
            {"id": "1", "value": "A"},
            {"id": "1", "value": "A"},  # Duplicate
            {"id": "2", "value": "B"}
        ])
        
        task = DuplicateCheckTask(action="keep")
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        task_output = result_context.get_task_output("duplicate_check")
        
        assert len(result_data) == 3  # All records kept
        assert task_output["duplicate_count"] == 1
        assert task_output["final_count"] == 3
    
    @pytest.mark.asyncio
    async def test_no_key_fields_specified(self):
        """Test duplicate check with no key fields (use all fields)."""
        test_data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Alice", "age": 25}  # Exact duplicate
        ]
        
        task = DuplicateCheckTask(
            key_fields=[],  # Empty means use all fields
            action="remove"
        )
        
        context = PipelineContext({"data": test_data})
        result_context = await task.execute(context)
        
        result_data = result_context.get("data")
        
        assert len(result_data) == 2  # Duplicate removed


@pytest.mark.asyncio
class TestIntegratedDataQuality:
    """Test integrated data quality workflow."""
    
    async def test_complete_quality_pipeline(self):
        """Test a complete data quality validation pipeline."""
        # Test data with various quality issues
        test_data = [
            {
                "id": "user_001",
                "email": "user1@example.com",
                "age": 25,
                "score": 85,
                "signup_date": "2024-01-01"
            },
            {
                "id": "user_002",
                "email": "user2@example.com", 
                "age": 30,
                "score": 92,
                "signup_date": "2024-01-02"
            },
            {
                "id": "user_001",  # Duplicate ID
                "email": "user1@example.com",
                "age": 25,
                "score": 85,
                "signup_date": "2024-01-01"
            },
            {
                "id": None,  # Invalid - null ID
                "email": "invalid-email",
                "age": -5,  # Invalid age
                "score": 150,  # Invalid score
                "signup_date": "2024-01-03"
            }
        ]
        
        # Step 1: Remove duplicates
        duplicate_checker = DuplicateCheckTask(
            key_fields=["id", "email"],
            action="remove"
        )
        
        # Step 2: Validate remaining data
        validator = DataQualityTask(
            rules=[
                NotNullRule("id"),
                PatternRule("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
                RangeRule("age", min_value=0, max_value=120),
                RangeRule("score", min_value=0, max_value=100)
            ],
            fail_on_error=False,
            error_threshold=0.5
        )
        
        # Execute pipeline
        context = PipelineContext({"data": test_data})
        
        # Remove duplicates
        context = await duplicate_checker.execute(context)
        
        # Validate quality
        context = await validator.execute(context)
        
        # Check results
        duplicate_output = context.get_task_output("duplicate_check")
        quality_output = context.get_task_output("data_quality_validation")
        
        # One duplicate should be removed
        assert duplicate_output["duplicate_count"] == 1
        assert duplicate_output["final_count"] == 3
        
        # Clean data should contain only valid records
        clean_data = context.get("clean_data")
        invalid_data = context.get("invalid_data")
        
        assert len(clean_data) == 2  # Only the two valid records
        assert len(invalid_data) == 1  # The invalid record
        
        # All clean records should have valid data
        for record in clean_data:
            assert record["id"] is not None
            assert "@" in record["email"]
            assert 0 <= record["age"] <= 120
            assert 0 <= record["score"] <= 100


if __name__ == "__main__":
    pytest.main([__file__])