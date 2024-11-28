import pandas as pd
from tqdm import tqdm

def update_epic_game_ids(input_file='epic_games.csv', output_file='epic_games_updated.csv'):
    try:
        print("Reading Epic Games dataset...")
        # Read Epic Games dataset
        epic_df = pd.read_csv(input_file)
        
        # Create new sequential IDs starting from 1
        print("Updating game IDs...")
        epic_df['game_id'] = range(1, len(epic_df) + 1)
        
        # Save the updated dataset
        print(f"Saving updated dataset to {output_file}...")
        epic_df.to_csv(output_file, index=False)
        
        print("\nUpdate Statistics:")
        print(f"Total games processed: {len(epic_df)}")
        print(f"ID range: 1 to {len(epic_df)}")
        
        # Display first few rows as example
        print("\nFirst 5 rows of updated dataset:")
        print(epic_df[['game_id', 'name']].head())
        
        return epic_df
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    # Create backup of original file
    try:
        print("Creating backup of original file...")
        original_df = pd.read_csv('epic_games.csv')
        original_df.to_csv('epic_games_backup.csv', index=False)
        print("Backup created as 'epic_games_backup.csv'")
    except Exception as e:
        print(f"Warning: Could not create backup: {str(e)}")
    
    # Update the IDs
    updated_df = update_epic_game_ids(output_file='epic_games.csv')