import pandas as pd
from tqdm import tqdm

def main():
    print("Reading wykonos-games.csv...")
    # Read the CSV file
    df = pd.read_csv('datasets/wykonos-games.csv')
    
    print("Removing rows with null game_ids...")
    # Remove rows where game_id is null
    initial_rows = len(df)
    df = df.dropna(subset=['game_id'])
    rows_removed = initial_rows - len(df)
    print(f"Removed {rows_removed} rows with null game_ids")
    
    print("Reordering columns...")
    # Get all column names
    columns = df.columns.tolist()
    
    # Remove game_id from the list
    columns.remove('game_id')
    
    # Create new column order with 'game_id' first
    new_columns = ['game_id'] + columns
    
    # Reorder columns
    df = df[new_columns]
    
    # Rename game_id to id
    df = df.rename(columns={'game_id': 'id'})
    
    print("Saving to new file...")
    # Save to a new file
    df.to_csv('datasets/wykonos-games.csv', index=False)
    
    print("\nVerification:")
    print(f"First column name: {df.columns[0]}")
    print(f"Total columns: {len(df.columns)}")
    print(f"Total rows: {len(df)}")
    print("\nDone! New file saved as 'wykonos-games.csv'")

if __name__ == "__main__":
    main() 