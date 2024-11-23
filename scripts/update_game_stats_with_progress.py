import pandas as pd
from tqdm import tqdm

def update_game_stats_with_favorites():
    # Read the CSV files
    print("Reading CSV files...")
    game_stats = pd.read_csv('newdata/game-stats.csv')
    player_stats = pd.read_csv('newdata/player_stats.csv')

    # Calculate favorites using groupby operation with progress bar
    print("Calculating favorites...")
    tqdm.pandas(desc="Processing games")
    favorites_by_game = player_stats.groupby('fk_gameid')['favorites'].progress_apply(sum).reset_index()
    favorites_by_game.columns = ['gameID', 'favorites']
    
    # Merge the results with game_stats
    game_stats = game_stats.merge(favorites_by_game, on='gameID', how='left')
    game_stats['favorites'] = game_stats['favorites'].fillna(0)
    
    # Save the updated DataFrame
    print("Saving updated CSV...")
    game_stats.to_csv('newdata/game-stats.csv', index=False)
    print("Done! Added favorites column to game-stats.csv")

if __name__ == "__main__":
    update_game_stats_with_favorites()