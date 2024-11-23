import pandas as pd
import uuid
from tqdm import tqdm

def generate_game_id():
    """Generate a game ID in the same format as existing IDs"""
    return str(uuid.uuid4()).replace('-', '')

def main():
    print("Reading final_games.csv...")
    # Read the CSV file
    df = pd.read_csv('datasets/final_games.csv')
    
    # Count null game_ids before processing
    null_count_before = df['game_id'].isna().sum()
    print(f"Found {null_count_before} games without IDs")
    
    print("Generating new IDs for games without game_ids...")
    # Generate new IDs only for rows where game_id is null
    for idx in tqdm(df.index[df['game_id'].isna()], desc="Generating IDs"):
        df.at[idx, 'game_id'] = generate_game_id()
    
    # Verify no null game_ids remain
    null_count_after = df['game_id'].isna().sum()
    print(f"\nVerification:")
    print(f"Games without IDs after processing: {null_count_after}")
    print(f"New IDs generated: {null_count_before - null_count_after}")
    
    print("\nSaving updated final_games.csv...")
    # Save the updated DataFrame back to CSV
    df.to_csv('datasets/newwwwwwwwwwwww.csv', index=False)
    print("Done!")

if __name__ == "__main__":
    main() 