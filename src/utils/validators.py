"""
Data validation functions for uploaded files
"""
import pandas as pd
from datetime import datetime
import re

class DataValidator:
    """Validates uploaded ticket data"""
    
    # Required columns
    REQUIRED_COLUMNS = ['created_at', 'text']
    
    # Optional columns
    OPTIONAL_COLUMNS = [
        'ticket_id', 'customer_id', 'category', 'channel', 
        'priority', 'customer_tier', 'product'
    ]
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def validate_file(self, df):
        """
        Validate uploaded DataFrame
        Returns: (is_valid, errors, warnings)
        """
        self.errors = []
        self.warnings = []
        
        # Check 1: Required columns
        self._check_required_columns(df)
        
        # Check 2: Data types
        self._check_data_types(df)
        
        # Check 3: Missing values
        self._check_missing_values(df)
        
        # Check 4: Data quality
        self._check_data_quality(df)
        
        is_valid = len(self.errors) == 0
        
        return is_valid, self.errors, self.warnings
    
    def _check_required_columns(self, df):
        """Check if required columns exist"""
        missing_cols = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        
        if missing_cols:
            self.errors.append(f"Missing required columns: {missing_cols}")
    
    def _check_data_types(self, df):
        """Validate data types"""
        if 'created_at' in df.columns:
            # Try to parse dates
            try:
                pd.to_datetime(df['created_at'])
            except Exception as e:
                self.errors.append(f"Invalid date format in 'created_at': {str(e)}")
        
        if 'text' in df.columns:
            # Check if text column contains strings
            if not df['text'].dtype == 'object':
                self.warnings.append("'text' column should contain text data")
    
    def _check_missing_values(self, df):
        """Check for missing values in required columns"""
        for col in self.REQUIRED_COLUMNS:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    self.errors.append(
                        f"Column '{col}' has {missing_count} missing values"
                    )
    
    def _check_data_quality(self, df):
        """Check data quality issues"""
        if 'text' in df.columns:
            # Check for very short text
            short_text = df[df['text'].str.len() < 10]
            if len(short_text) > 0:
                self.warnings.append(
                    f"{len(short_text)} tickets have very short text (<10 characters)"
                )
            
            # Check for empty text
            empty_text = df[df['text'].str.strip() == '']
            if len(empty_text) > 0:
                self.errors.append(
                    f"{len(empty_text)} tickets have empty text"
                )
        
        if 'created_at' in df.columns:
            try:
                dates = pd.to_datetime(df['created_at'])
                
                # Check for future dates
                future_dates = dates > datetime.now()
                if future_dates.sum() > 0:
                    self.warnings.append(
                        f"{future_dates.sum()} tickets have future dates"
                    )
                
                # Check for very old dates
                old_dates = dates < datetime(2020, 1, 1)
                if old_dates.sum() > 0:
                    self.warnings.append(
                        f"{old_dates.sum()} tickets are older than 2020"
                    )
            except:
                pass  # Already caught in data type check
    
    def get_validation_report(self):
        """Get formatted validation report"""
        report = []
        
        if self.errors:
            report.append("❌ ERRORS:")
            for error in self.errors:
                report.append(f"  • {error}")
        
        if self.warnings:
            report.append("\n⚠️  WARNINGS:")
            for warning in self.warnings:
                report.append(f"  • {warning}")
        
        if not self.errors and not self.warnings:
            report.append("✅ All validation checks passed!")
        
        return "\n".join(report)


def validate_ticket_data(df):
    """
    Main validation function
    Returns: (is_valid, report_text)
    """
    validator = DataValidator()
    is_valid, errors, warnings = validator.validate_file(df)
    report = validator.get_validation_report()
    
    return is_valid, report