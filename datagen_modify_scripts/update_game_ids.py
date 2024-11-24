import pandas as pd

# Read the CSV files
final_games_df = pd.read_csv('datasets/FINALGAMES.csv')
wykonos_df = pd.read_csv('datasets/wykonos_epic_games_with_ids.csv')

# Create a dictionary mapping game names to their IDs from FINALGAMES.csv
game_id_mapping = dict(zip(final_games_df['Name'], final_games_df['id']))

# Function to clean game names for better matching
def clean_game_name(name):
    if pd.isna(name):
        return name
    return str(name).lower().strip()

# Clean game names in both dataframes
final_games_df['clean_name'] = final_games_df['Name'].apply(clean_game_name)
wykonos_df['clean_name'] = wykonos_df['name'].apply(clean_game_name)

# Create updated mapping with cleaned names
clean_game_id_mapping = dict(zip(final_games_df['clean_name'], final_games_df['id']))

# Update game IDs based on matching names
wykonos_df['game_id'] = wykonos_df['clean_name'].map(clean_game_id_mapping)

# Remove the temporary clean_name column
wykonos_df = wykonos_df.drop('clean_name', axis=1)

# Save the updated DataFrame to CSV
wykonos_df.to_csv('datasets/wykonos_epic_games_with_ids.csv', index=False)

# Print some statistics
total_games = len(wykonos_df)
games_with_ids = wykonos_df['game_id'].notna().sum()
print(f"Total games: {total_games}")
print(f"Games matched with IDs: {games_with_ids}")
print(f"Games without matches: {total_games - games_with_ids}") 