import pandas as pd
from tqdm import tqdm

def update_game_stats_with_favorites():
    # Read the CSV files
    print("Reading CSV files...")
    game_stats = pd.read_csv('newdata/game-stats.csv')
    player_stats = pd.read_csv('newdata/player_stats.csv')

    # Initialize a dictionary to store favorites count for each game
    favorites_count = {}
    
    # Group by game ID and count favorites
    print("Calculating favorites...")
    favorites_count = player_stats.groupby('fk_gameid')['favorites'].sum().to_dict()
    
    # Add favorites column to game_stats
    print("Updating game stats...")
    game_stats['favorites'] = game_stats['gameID'].map(favorites_count).fillna(0).astype(int)
    
    # Save the updated DataFrame
    print("Saving updated CSV...")
    game_stats.to_csv('newdata/game-stats.csv', index=False)
    print("Done! Added favorites column to game-stats.csv")

if __name__ == "__main__":
    update_game_stats_with_favorites() 