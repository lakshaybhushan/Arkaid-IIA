import pandas as pd
import numpy as np
from tqdm import tqdm
import random
from collections import defaultdict

def update_hours_and_favorites():
    print("Reading datasets...")
    player_stats_df = pd.read_csv('newdata/player_stats.csv')
    game_stats_df = pd.read_csv('newdata/game-stats.csv')

    # Create a dictionary for quick game stats lookup
    game_hours = dict(zip(game_stats_df['gameID'], game_stats_df['hrsPlayed']))

    def generate_new_hours(game_id):
        # Get reference hours from game_stats
        ref_hours = game_hours.get(game_id, 200)
        if pd.isna(ref_hours) or ref_hours <= 0:
            ref_hours = 200

        # Use more extreme shape parameters for stronger skew
        # alpha=1.2 gives strong skew towards lower values
        # beta is adjusted based on reference hours
        hours = random.gammavariate(alpha=1.2, beta=ref_hours/20)
        
        # Add occasional spike for "addicted" players (5% chance)
        if random.random() < 0.05:
            hours *= random.uniform(2, 4)

        return min(round(hours, 2), 1000)  # Cap at 1000 hours

    print("Updating hours played...")
    # Update hours using vectorized operations
    tqdm.pandas(desc="Generating new hours")
    player_stats_df['hrs_played'] = player_stats_df['fk_gameid'].progress_apply(generate_new_hours)

    print("Calculating favorites...")
    # Group by player to find their most played games
    player_favorites = defaultdict(set)
    
    # Calculate number of favorite games per player (between 1-3 based on their total games)
    player_game_counts = player_stats_df.groupby('playerId').size()
    
    for player_id in tqdm(player_stats_df['playerId'].unique(), desc="Processing favorites"):
        player_games = player_stats_df[player_stats_df['playerId'] == player_id]
        num_games = len(player_games)
        
        # Determine number of favorites (more games = more potential favorites)
        num_favorites = min(max(1, num_games // 10), 3)
        
        # Get the indices of top played games
        top_games_idx = player_games.nlargest(num_favorites, 'hrs_played').index
        player_favorites[player_id].update(top_games_idx)

    # Create favorites column
    print("Adding favorites column...")
    player_stats_df['favorites'] = 0
    for player_id, favorite_indices in player_favorites.items():
        player_stats_df.loc[list(favorite_indices), 'favorites'] = 1

    print("Saving updated CSV...")
    player_stats_df.to_csv('newdata/player_stats.csv', index=False)
    print("Update complete!")

if __name__ == "__main__":
    update_hours_and_favorites() 