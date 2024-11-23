import pandas as pd
import numpy as np
from tqdm import tqdm
import random

def update_ratings():
    print("Reading datasets...")
    # Read the existing player_stats and game_stats
    player_stats_df = pd.read_csv('datasets/player_stats.csv')
    game_stats_df = pd.read_csv('newdata/game-stats.csv')

    print("Updating ratings...")
    # Create a dictionary for quick game stats lookup
    game_ratings = dict(zip(game_stats_df['gameID'], game_stats_df['avg_user_rating']))

    # Function to generate new rating
    def generate_new_rating(game_id):
        avg_rating = game_ratings.get(game_id, 70)
        if pd.isna(avg_rating):
            avg_rating = 70
        rating_variation = random.uniform(-20, 20)
        return round(max(0, min(100, avg_rating + rating_variation)))

    # Update ratings using vectorized operations for speed
    tqdm.pandas(desc="Generating new ratings")
    player_stats_df['rating'] = player_stats_df['fk_gameid'].progress_apply(generate_new_rating)

    print("Saving updated CSV...")
    player_stats_df.to_csv('datasets/player_stats.csv', index=False)
    print("Ratings update complete!")

if __name__ == "__main__":
    update_ratings() 