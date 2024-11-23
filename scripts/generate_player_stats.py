import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
import random

# Initialize Faker
fake = Faker()

def generate_player_stats():
    # Read the required datasets
    print("Reading datasets...")
    game_stats_df = pd.read_csv('newdata/game-stats.csv')
    player_sales_df = pd.read_csv('datasets/player_sales.csv')

    # Get unique player-game combinations
    print("Getting unique player-game combinations...")
    unique_combinations = player_sales_df[['GameID', 'PlayerID']].drop_duplicates()

    # Create empty lists to store data
    player_stats_data = []

    # Process each player-game combination
    print("Generating player statistics...")
    for _, row in tqdm(unique_combinations.iterrows(), total=len(unique_combinations)):
        game_id = row['GameID']
        player_id = row['PlayerID']

        # Get game details from game_stats
        game_info = game_stats_df[game_stats_df['gameID'] == game_id].iloc[0]

        # Generate hours played based on the game's average hours
        # If hrsPlayed is available in game_stats, use it as a reference
        max_hrs = game_info.get('hrsPlayed', 1000)
        if pd.isna(max_hrs) or max_hrs <= 0:
            max_hrs = 1000
        
        # Generate hours with a slight bias towards lower values
        hrs_played = random.gammavariate(2, max_hrs/10)
        hrs_played = min(hrs_played, 1000)  # Cap at 1000 hours

        # Generate achievements
        max_achievements = game_info.get('Achievements', 100)
        if pd.isna(max_achievements) or max_achievements <= 0:
            max_achievements = 50
        achievements_completed = random.randint(0, int(max_achievements))

        # Generate rating based on avg_user_rating
        avg_rating = game_info.get('avg_user_rating', 70)  # Default is 70 on 0-100 scale
        if pd.isna(avg_rating):
            avg_rating = 70
        
        # Add variation around the average rating
        rating_variation = random.uniform(-20, 20)  # Variation of ±20 points
        player_rating = max(0, min(100, avg_rating + rating_variation))

        # Add to data list
        player_stats_data.append({
            'playerId': player_id,
            'fk_gameid': game_id,
            'hrs_played': round(hrs_played, 2),
            'achievements_completed': achievements_completed,
            'rating': round(player_rating, 0)  # Rounded to whole number
        })

    # Create DataFrame
    print("Creating DataFrame...")
    player_stats_df = pd.DataFrame(player_stats_data)

    # Save to CSV
    print("Saving to CSV...")
    player_stats_df.to_csv('datasets/player_stats.csv', index=False)
    print("Player stats generation complete!")

if __name__ == "__main__":
    generate_player_stats() 