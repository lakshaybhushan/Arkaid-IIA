import pandas as pd
from tqdm import tqdm

def find_matching_games(epic_file='epic_games.csv', steam_file='steam_games.csv'):
    try:
        print("Reading datasets...")
        # Read Epic Games dataset
        epic_df = pd.read_csv(epic_file)
        
        # Read Steam Games dataset
        steam_df = pd.read_csv(steam_file)
        
        # Convert game names to lowercase for better matching
        epic_games = set(epic_df['name'].str.lower().str.strip())
        steam_games = set(steam_df['Name'].str.lower().str.strip())
        
        # Find matching games
        matching_games = epic_games.intersection(steam_games)
        
        # Create a DataFrame with matching games
        matching_df = pd.DataFrame({
            'Game Name': sorted(list(matching_games))
        })
        
        # Save matching games to CSV
        output_file = 'matching_games.csv'
        matching_df.to_csv(output_file, index=False)
        
        print("\nMatching Games Statistics:")
        print(f"Total Epic Games: {len(epic_games)}")
        print(f"Total Steam Games: {len(steam_games)}")
        print(f"Number of Matching Games: {len(matching_games)}")
        print(f"\nMatching games have been saved to {output_file}")
        
        # Display first 10 matching games as example
        if len(matching_games) > 0:
            print("\nFirst 10 matching games:")
            for game in sorted(list(matching_games))[:10]:
                print(f"- {game}")
        
        return matching_df
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    matching_df = find_matching_games() 