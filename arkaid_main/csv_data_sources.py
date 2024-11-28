import pandas as pd
import os
from datetime import datetime
import numpy as np

def get_column_type(column):
    """Determine the data type of a column"""
    
    # Check if column is entirely null
    if column.isnull().all():
        return "NULL"
    
    # Get non-null values
    sample = column.dropna()
    if len(sample) == 0:
        return "NULL"
        
    # Check if dates
    if pd.api.types.is_datetime64_any_dtype(sample):
        return "date"
    
    # Try parsing as date if string
    if sample.dtype == object:
        try:
            pd.to_datetime(sample.iloc[0])
            return "date"
        except:
            pass
    
    # Check numeric types
    if np.issubdtype(sample.dtype, np.integer):
        return "integer"
    elif np.issubdtype(sample.dtype, np.floating):
        return "float"
    elif sample.dtype == bool:
        return "boolean"
    
    # Check if comma-separated list
    if sample.dtype == object and sample.iloc[0] and ',' in str(sample.iloc[0]):
        return "list"
        
    # Default to string for object type
    return "string"

def analyze_csv_datatypes(folder_path="data"):
    """Analyze data types for all CSV files in the specified folder"""
    
    results = {}
    
    # Get all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    for file in csv_files:
        print(f"\nAnalyzing {file}...")
        
        # Read CSV file
        df = pd.read_csv(os.path.join(folder_path, file))
        
        # Get data types for each column
        file_types = {}
        for column in df.columns:
            dtype = get_column_type(df[column])
            file_types[column] = dtype
            
        results[file] = file_types
        
    return results

def format_results(results):
    """Format results into a readable string"""
    output = []
    
    for file, columns in results.items():
        output.append(f"\n{file} datatypes:")
        output.append("-" * 50)
        
        # Get max column name length for alignment
        max_len = max(len(col) for col in columns.keys())
        
        for column, dtype in columns.items():
            # Pad column name for alignment
            padded_col = column.ljust(max_len)
            output.append(f"{padded_col} : {dtype}")
            
    return "\n".join(output)

def main():
    # Analyze all CSV files
    results = analyze_csv_datatypes()
    
    # Format and print results
    formatted_output = format_results(results)
    print(formatted_output)
    
    # Save results to file
    with open('data_types.txt', 'w') as f:
        f.write(formatted_output)
    
    print("\nResults have been saved to data_types.txt")

if __name__ == "__main__":
    main() 