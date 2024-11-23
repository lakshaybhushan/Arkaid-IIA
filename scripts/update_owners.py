import pandas as pd
from tqdm import tqdm
import numpy as np

def update_estimated_owners():
    print("Loading datasets...")
    
    # Read the CSV files
    steam_games = pd.read_csv('datasets/steam-games.csv')
    player_sales = pd.read_csv('datasets/player_sales.csv')
    
    print("Calculating sales counts...")
    
    # Group by GameID and count the number of sales
    sales_counts = player_sales.groupby('GameID').size().reset_index(name='actual_owners')
    
    # Create a dictionary for faster lookup
    sales_dict = dict(zip(sales_counts['GameID'], sales_counts['actual_owners']))
    
    print("Updating estimated owners...")
    
    # Function to update a single row's estimated owners
    def update_owners(row):
        game_id = row['id']
        if game_id in sales_dict:
            return str(sales_dict[game_id])  # Convert to string to match original format
        return row['Estimated owners']  # Keep original if no sales data
    
    # Apply the update with progress bar
    tqdm.pandas(desc="Processing games")
    steam_games['Estimated owners'] = steam_games.progress_apply(update_owners, axis=1)
    
    print("Saving updated dataset...")
    
    # Save the updated dataset
    steam_games.to_csv('datasets/steam-games.csv', index=False)
    
    print("Complete! Updated estimated owners based on actual sales data.")

if __name__ == "__main__":
    try:
        update_estimated_owners()
    except Exception as e:
        print(f"An error occurred: {str(e)}") 