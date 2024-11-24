import pandas as pd
from tqdm import tqdm
import os

def remove_library_columns(input_file: str) -> None:
    """
    Remove Library_Title and Library_Playing columns from players_main.csv
    
    Args:
        input_file: Path to the players_main.csv file
    """
    print(f"Reading {input_file}...")
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # List of columns to remove
    columns_to_remove = ['Library_Title', 'Library_Playing']
    
    # Remove the columns
    print("Removing Library_Title and Library_Playing columns...")
    df_cleaned = df.drop(columns=columns_to_remove)
    
    # Save the modified dataframe back to the same file
    print("Saving updated CSV file...")
    with tqdm(total=1, desc="Saving") as pbar:
        df_cleaned.to_csv(input_file, index=False)
        pbar.update(1)
    
    # Print statistics
    print("\nColumns removed successfully!")
    print(f"Original number of columns: {len(df.columns)}")
    print(f"New number of columns: {len(df_cleaned.columns)}")
    print(f"Removed columns: {', '.join(columns_to_remove)}")

if __name__ == "__main__":
    # Define input path
    INPUT_FILE = "/Users/lakshaybhushan/Developer/IIAProject-ArkAid/datasets/split_tables/players_main.csv"
    
    remove_library_columns(INPUT_FILE) 