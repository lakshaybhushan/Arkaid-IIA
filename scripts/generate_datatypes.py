import os
import pandas as pd
from datetime import datetime

def detect_datatype(series):
    """Detect the datatype of a pandas series"""
    if series.dtype == 'object':
        # Check if it's a date
        try:
            pd.to_datetime(series.dropna().iloc[0])
            return 'date'
        except (TypeError, ValueError):
            # Check if it's a boolean
            if series.dropna().isin([True, False, 0, 1, 'True', 'False']).all():
                return 'boolean'
            return 'string'
    elif series.dtype == 'int64':
        return 'integer'
    elif series.dtype == 'float64':
        return 'float'
    else:
        return str(series.dtype)

def analyze_csv_files(folder_path):
    """Analyze all CSV files in the given folder and return their datatypes"""
    results = []
    
    # Walk through the folder
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    # Read the CSV file
                    df = pd.read_csv(file_path)
                    
                    # Get datatypes for each column
                    datatypes = {}
                    for column in df.columns:
                        datatypes[column] = detect_datatype(df[column])
                    
                    results.append({
                        'file': file,
                        'datatypes': datatypes
                    })
                except Exception as e:
                    print(f"Error reading {file}: {str(e)}")
    
    return results

def write_datatypes_file(results, output_file):
    """Write the results to a text file in the specified format"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("Column Datatypes for CSV Files in Datasets Folder\n")
        f.write("==================================================\n\n")
        
        for result in results:
            f.write(f"File: {result['file']}\n")
            f.write("-" * 30 + "\n")
            
            for column, datatype in result['datatypes'].items():
                f.write(f"{column}: {datatype}\n")
            
            f.write("\n")

def main():
    # Specify the datasets folder path
    datasets_folder = "datasets"
    output_file = "generated_datatypes.txt"
    
    # Create datasets folder if it doesn't exist
    if not os.path.exists(datasets_folder):
        print(f"Error: {datasets_folder} folder not found")
        return
    
    # Analyze CSV files
    print("Analyzing CSV files...")
    results = analyze_csv_files(datasets_folder)
    
    # Write results to file
    print(f"Writing results to {output_file}...")
    write_datatypes_file(results, output_file)
    
    print("Done!")

if __name__ == "__main__":
    main() 