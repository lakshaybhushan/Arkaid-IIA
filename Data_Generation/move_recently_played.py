import pandas as pd
from tqdm import tqdm
import os

def move_recently_played(main_file: str, virtual_file: str) -> None:
    """
    Extract RecentlyPlayed column from players_main.csv and add it to players_virtual.csv
    
    Args:
        main_file: Path to the players_main.csv file
        virtual_file: Path to the players_virtual.csv file
    """
    print("Reading input files...")
    
    # Read the CSV files
    df_main = pd.read_csv(main_file)
    
    # Check if virtual file exists, if not create empty DataFrame
    if os.path.exists(virtual_file):
        df_virtual = pd.read_csv(virtual_file)
    else:
        df_virtual = pd.DataFrame()
        if 'ID' in df_main.columns:
            df_virtual['ID'] = df_main['ID']
    
    # Extract RecentlyPlayed column
    recently_played_col = df_main['RecentlyPlayed']
    
    # Remove RecentlyPlayed from main file
    print("Removing RecentlyPlayed from players_main.csv...")
    df_main_updated = df_main.drop(columns=['RecentlyPlayed'])
    
    # Add RecentlyPlayed to virtual file
    print("Adding RecentlyPlayed to players_virtual.csv...")
    df_virtual['RecentlyPlayed'] = recently_played_col
    
    # Save the modified dataframes
    print("Saving updated files...")
    with tqdm(total=2, desc="Saving files") as pbar:
        # Save updated main file
        df_main_updated.to_csv(main_file, index=False)
        pbar.update(1)
        
        # Save updated virtual file
        df_virtual.to_csv(virtual_file, index=False)
        pbar.update(1)
    
    # Print statistics
    print("\nColumn moved successfully!")
    print(f"Main file columns: {len(df_main_updated.columns)}")
    print(f"Virtual file columns: {len(df_virtual.columns)}")
    print("RecentlyPlayed column has been moved to players_virtual.csv")

if __name__ == "__main__":
    # Define file paths
    MAIN_FILE = "/Users/lakshaybhushan/Developer/IIAProject-ArkAid/datasets/split_tables/players_main.csv"
    VIRTUAL_FILE = "/Users/lakshaybhushan/Developer/IIAProject-ArkAid/datasets/split_tables/players_virtual.csv"
    
    move_recently_played(MAIN_FILE, VIRTUAL_FILE) 