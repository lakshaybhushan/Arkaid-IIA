import pandas as pd
from tqdm import tqdm
import os

def split_players_table(input_file: str, output_dir: str) -> None:
    """
    Split players.csv into two tables:
    1. players_main - Contains all columns except financial/gaming metrics
    2. players_gaming_metrics - Contains ID and gaming/financial related columns
    
    Args:
        input_file: Path to the input players.csv file
        output_dir: Directory to save the output CSV files
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the CSV file with specified dtypes for optimization
    dtypes = {
        'ID': str,
        'List_of_SaleIDs': str,
        'Total_Hours_Played': float,
        'Wallet': float,
        'Cart': str
    }
    
    print(f"Reading players.csv from {input_file}...")
    df = pd.read_csv(input_file, dtype=dtypes)
    
    # Define columns for gaming metrics table
    gaming_metrics_columns = ['ID', 'List_of_SaleIDs', 'Total_Hours_Played', 'Wallet', 'Cart']
    
    # Create gaming metrics dataframe
    print("Creating players_gaming_metrics.csv...")
    df_gaming_metrics = df[gaming_metrics_columns].copy()
    
    # Create main players dataframe (excluding gaming metrics columns except ID)
    print("Creating players_main.csv...")
    main_columns = [col for col in df.columns if col not in gaming_metrics_columns[1:]]
    df_main = df[main_columns].copy()
    
    # Save to CSV files with progress bars
    print("Saving files...")
    
    # Save gaming metrics table
    with tqdm(total=1, desc="Saving gaming metrics") as pbar:
        df_gaming_metrics.to_csv(
            os.path.join(output_dir, 'players_gaming_metrics.csv'),
            index=False
        )
        pbar.update(1)
    
    # Save main players table
    with tqdm(total=1, desc="Saving main table") as pbar:
        df_main.to_csv(
            os.path.join(output_dir, 'players_main.csv'),
            index=False
        )
        pbar.update(1)
    
    # Print some statistics
    print("\nSplit completed successfully!")
    print(f"Original table shape: {df.shape}")
    print(f"Main table shape: {df_main.shape}")
    print(f"Gaming metrics table shape: {df_gaming_metrics.shape}")

if __name__ == "__main__":
    # Define input and output paths using absolute path
    INPUT_FILE = "/Users/lakshaybhushan/Developer/IIAProject-ArkAid/datasets/players.csv"
    OUTPUT_DIR = "/Users/lakshaybhushan/Developer/IIAProject-ArkAid/datasets/split_tables"
    
    split_players_table(INPUT_FILE, OUTPUT_DIR) 