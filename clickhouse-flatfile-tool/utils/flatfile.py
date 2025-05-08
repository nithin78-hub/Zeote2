import csv
import pandas as pd
import os

class FlatFileManager:
    def __init__(self, filepath, delimiter=','):
        """
        Initialize a flat file manager with file path and delimiter.
        """
        self.filepath = filepath
        self.delimiter = delimiter
        
        # Validate file existence
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
    
    def get_columns(self):
        """
        Get column names from the file header.
        """
        try:
            # Try pandas for more robust handling
            df = pd.read_csv(self.filepath, delimiter=self.delimiter, nrows=0)
            columns = []
            
            for col in df.columns:
                # Get data type from first few rows if possible
                dtype = 'String'  # Default
                try:
                    sample_df = pd.read_csv(self.filepath, delimiter=self.delimiter, usecols=[col], nrows=10)
                    if pd.api.types.is_numeric_dtype(sample_df[col]):
                        if pd.api.types.is_integer_dtype(sample_df[col]):
                            dtype = 'Int64'
                        else:
                            dtype = 'Float64'
                    elif pd.api.types.is_datetime64_dtype(sample_df[col]):
                        dtype = 'DateTime'
                    elif pd.api.types.is_bool_dtype(sample_df[col]):
                        dtype = 'Boolean'
                except:
                    # If error occurs, keep the default
                    pass
                
                columns.append({
                    'name': col,
                    'type': dtype
                })
            
            return columns
        except Exception as e:
            # Fallback to CSV module
            with open(self.filepath, 'r', newline='') as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                header = next(reader)
                
                return [{'name': col, 'type': 'String'} for col in header]
    
    def preview_data(self, columns=None, limit=100):
        """
        Get preview data for selected columns.
        """
        try:
            # Use only selected columns if specified
            usecols = columns if columns and len(columns) > 0 else None
            
            # Read data with pandas
            df = pd.read_csv(self.filepath, delimiter=self.delimiter, 
                             nrows=limit, usecols=usecols)
            
            # Convert to dictionary format for JSON response
            return df.to_dict('records')
        except Exception as e:
            raise ValueError(f"Failed to preview data: {str(e)}")
    
    def get_data(self, columns=None):
        """
        Get data as pandas DataFrame with selected columns.
        """
        try:
            # Use only selected columns if specified
            usecols = columns if columns and len(columns) > 0 else None
            
            # Read data with pandas
            df = pd.read_csv(self.filepath, delimiter=self.delimiter, usecols=usecols)
            
            return df
        except Exception as e:
            raise ValueError(f"Failed to get data: {str(e)}")
    
    def count_rows(self):
        """
        Count the total number of rows in the file (excluding header).
        """
        try:
            # Fast way to count rows
            with open(self.filepath, 'r') as f:
                return sum(1 for _ in f) - 1  # Subtract 1 for header
        except Exception as e:
            raise ValueError(f"Failed to count rows: {str(e)}")