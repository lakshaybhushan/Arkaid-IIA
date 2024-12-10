import pandas as pd
from tqdm import tqdm

def update_creators_and_modders(steam_file='steam_games.csv',
                              epic_file='old_epic_games.csv',
                              creators_file='content_creators.csv',
                              modders_file='modders.csv',
                              n_creators=2000,
                              n_modders=500):
    try:
        print("Reading datasets...")
        # Read all datasets
        steam_df = pd.read_csv(steam_file)
        epic_df = pd.read_csv(epic_file)
        creators_df = pd.read_csv(creators_file)
        modders_df = pd.read_csv(modders_file)
        
        # Create game ID to name mapping from both Steam and Epic
        print("Creating game ID to name mapping...")
        game_id_map = {}
        
        # Add Steam games to mapping
        for _, row in tqdm(steam_df.iterrows(), desc="Processing Steam games"):
            game_id_map[str(row['id']).strip()] = row['Name'].strip()
            
        # Add Epic games to mapping
        for _, row in tqdm(epic_df.iterrows(), desc="Processing Epic games"):
            game_id_map[str(row['game_id']).strip()] = row['name'].strip()
            
        print("\nUpdating content creators...")
        # Convert Primary_Game_ID to string for matching
        creators_df['Primary_Game_ID'] = creators_df['Primary_Game_ID'].astype(str).str.strip()
        
        # Create game name column and filter invalid games
        creators_df['Game_Name'] = creators_df['Primary_Game_ID'].map(game_id_map)
        creators_filtered = creators_df.dropna(subset=['Game_Name'])
        
        # Sample required number of creators
        if len(creators_filtered) > n_creators:
            creators_filtered = creators_filtered.sample(n=n_creators, random_state=42)
        
        # Remove old ID column and rename Game_Name to Primary_Game
        creators_filtered = creators_filtered.drop(columns=['Primary_Game_ID'])
        creators_filtered = creators_filtered.rename(columns={'Game_Name': 'Primary_Game'})
        
        print("\nUpdating modders...")
        # Convert game_id to string for matching
        modders_df['Primary_Game_ID'] = modders_df['Primary_Game_ID'].astype(str).str.strip()
        
        # Create game name column and filter invalid games
        modders_df['Game_Name'] = modders_df['Primary_Game_ID'].map(game_id_map)
        modders_filtered = modders_df.dropna(subset=['Game_Name'])
        
        # Sample required number of modders
        if len(modders_filtered) > n_modders:
            modders_filtered = modders_filtered.sample(n=n_modders, random_state=42)
        
        # Remove old ID column and rename Game_Name to game
        modders_filtered = modders_filtered.drop(columns=['Primary_Game_ID'])
        modders_filtered = modders_filtered.rename(columns={'Game_Name': 'Primary_Game'})
        
        # Save updated datasets
        print("\nSaving updated datasets...")
        creators_filtered.to_csv('content_creators_updated.csv', index=False)
        modders_filtered.to_csv('modders_updated.csv', index=False)
        
        # Create backups of original files
        creators_df.to_csv('content_creators_backup.csv', index=False)
        modders_df.to_csv('modders_backup.csv', index=False)
        
        # Print statistics
        print("\nUpdate Statistics:")
        print(f"Content Creators:")
        print(f"- Original count: {len(creators_df)}")
        print(f"- After filtering invalid games: {len(creators_df[creators_df['Primary_Game_ID'].map(game_id_map).notna()])}")
        print(f"- Final count: {len(creators_filtered)} (limited to {n_creators})")
        
        print(f"\nModders:")
        print(f"- Original count: {len(modders_df)}")
        print(f"- After filtering invalid games: {len(modders_df[modders_df['Primary_Game_ID'].map(game_id_map).notna()])}")
        print(f"- Final count: {len(modders_filtered)} (limited to {n_modders})")
        
        return creators_filtered, modders_filtered
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None, None

if __name__ == "__main__":
    # Update creators and modders with size limits
    creators_df, modders_df = update_creators_and_modders(n_creators=2000, n_modders=500)
    
    if creators_df is not None and modders_df is not None:
        print("\nUpdate completed successfully!")
        print("New files created:")
        print("- content_creators_updated.csv")
        print("- modders_updated.csv")
        print("Backups created:")
        print("- content_creators_backup.csv")
        print("- modders_backup.csv") 