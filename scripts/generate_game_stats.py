import pandas as pd
import numpy as np
from tqdm import tqdm
import re
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, as_completed

pd.options.mode.chained_assignment = None  # default='warn'
# Initialize Faker
fake = Faker()

# Helper function to convert Steam's owner range to approximate number
def parse_owners(owners_str):
    if pd.isna(owners_str):
        return 0
    numbers = re.findall(r'\d+', owners_str)
    if len(numbers) >= 2:
        return (int(numbers[0]) + int(numbers[1])) // 2
    return 0

# Helper function to determine difficulty level based on completion time
def get_difficulty_level(hours):
    if pd.isna(hours) or hours < 10:
        return 'Low'
    elif hours < 30:
        return 'Med'
    else:
        return 'High'

# New helper functions for synthetic data generation
def generate_synthetic_playtime():
    """Generate realistic playtime hours with a right-skewed distribution"""
    return max(0, np.random.lognormal(2, 1.2))

def generate_synthetic_completion_rate():
    """Generate realistic completion rate with bias towards lower values"""
    return min(1.0, max(0.0, np.random.beta(2, 5)))

def generate_synthetic_active_users():
    """Generate realistic daily active users count"""
    return int(np.random.lognormal(8, 2))

def generate_synthetic_critic_rating():
    """Generate realistic critic rating between 0-100"""
    return min(100, max(0, np.random.normal(75, 15)))

def generate_synthetic_user_rating():
    """Generate realistic user rating between 0-100"""
    return min(100, max(0, np.random.normal(70, 20)))

def get_esrb_rating(age_rating):
    """Convert numerical age rating to ESRB rating"""
    if pd.isna(age_rating) or age_rating == 0:
        return np.random.choice(['E', 'E10+', 'T', 'M', 'A'], p=[0.2, 0.2, 0.2, 0.3, 0.1])
    elif age_rating <= 6:
        return 'E'
    elif age_rating <= 10:
        return 'E10+'
    elif age_rating <= 13:
        return 'T'
    elif age_rating <= 17:
        return 'M'
    else:
        return 'A'

def generate_realistic_playtime(total_users):
    """Generate realistic playtime based on user count"""
    if total_users == 0:
        return 0
    # Average hours per user increases with popularity but with diminishing returns
    avg_hours_per_user = np.random.lognormal(2, 0.5) * (1 + np.log1p(total_users/1000))
    return max(total_users, int(avg_hours_per_user * total_users))

def generate_realistic_rating():
    """Generate realistic rating between 0-100 with a bias towards middle-high scores"""
    # Games that make it to platforms tend to be at least decent
    base_rating = np.random.beta(7, 3) * 100  # This creates a distribution peaked around 70-80
    return round(base_rating, 1)

# Add this new helper function with the other helpers
def generate_synthetic_genres():
    """Generate realistic game genres when missing"""
    common_genres = [
        'Action', 'Adventure', 'RPG', 'Strategy', 'Simulation', 
        'Sports', 'Racing', 'Puzzle', 'Casual', 'Indie'
    ]
    # Return 1-3 random genres
    num_genres = np.random.randint(1, 4)
    genres = np.random.choice(common_genres, size=num_genres, replace=False)
    return ', '.join(genres)

def process_game_batch(games_batch, open_critic_df, wykonos_df, is_steam=True):
    """Process a batch of games in parallel"""
    results = []
    for game in games_batch.itertuples():
        if is_steam:
            critic_ratings = open_critic_df[open_critic_df['game_id'] == game.id]['rating'].tolist()
            game_name = str(game.Name) if not pd.isna(game.Name) else ''
            genres = game.Genres if hasattr(game, 'Genres') and not pd.isna(game.Genres) else generate_synthetic_genres()
        else:
            critic_ratings = open_critic_df[open_critic_df['game_id'] == game.game_id]['rating'].tolist()
            game_name = str(game.name) if not pd.isna(game.name) else ''
            genres = game.genres if hasattr(game, 'genres') and not pd.isna(game.genres) else generate_synthetic_genres()

        wykonos_match = wykonos_df[wykonos_df['name'].str.lower() == game_name.lower()].iloc[0] if len(wykonos_df[wykonos_df['name'].str.lower() == game_name.lower()]) > 0 else None

        if is_steam:
            total_users = parse_owners(game.Estimated_owners) if hasattr(game, 'Estimated_owners') else int(np.random.lognormal(10, 2))
            hrs_played = game.Average_playtime_forever if hasattr(game, 'Average_playtime_forever') and not pd.isna(game.Average_playtime_forever) else generate_realistic_playtime(total_users)
            daily_active = game.Peak_CCU if hasattr(game, 'Peak_CCU') and not pd.isna(game.Peak_CCU) else generate_synthetic_active_users()
        else:
            total_users = int(np.random.lognormal(10, 2))
            hrs_played = generate_realistic_playtime(total_users)
            daily_active = generate_synthetic_active_users()

        game_data = {
            'gameID': game.id if is_steam else game.game_id,
            'Name': game_name,
            'Genres': genres,
            'Achievements': game.Achievements if is_steam and hasattr(game, 'Achievements') and not pd.isna(game.Achievements) else int(np.random.normal(30, 15)),
            'Difficulty Level': get_difficulty_level(wykonos_match['to_beat_main'] if wykonos_match is not None else None),
            'hrsPlayed': max(hrs_played, total_users),
            'avg_critic_rating': np.mean(critic_ratings) if critic_ratings else generate_realistic_rating(),
            'avg_user_rating': game.User_score if is_steam and hasattr(game, 'User_score') and not pd.isna(game.User_score) else generate_realistic_rating(),
            'total_users': total_users,
            'total_critics': len(critic_ratings) or int(np.random.normal(50, 20)),
            'age_rating': get_esrb_rating(game.Required_age if is_steam and hasattr(game, 'Required_age') else None),
            'completion_rate': generate_synthetic_completion_rate(),
            'daily_active_users': daily_active,
            'monthly_active_users': daily_active * np.random.randint(20, 35)
        }
        results.append(game_data)
    return results

def main():
    # Load datasets
    print("Loading datasets...")
    steam_df = pd.read_csv('datasets/steam-games.csv')
    epic_df = pd.read_csv('datasets/epic-games.csv')
    open_critic_df = pd.read_csv('newdata/open_critic.csv')
    wykonos_df = pd.read_csv('datasets/wykonos-games.csv')

    # Convert column names to match the access pattern
    steam_df.columns = [c.replace(' ', '_') for c in steam_df.columns]
    epic_df.columns = [c.replace(' ', '_') for c in epic_df.columns]

    game_stats = []
    
    # Number of threads to use
    n_threads = 8
    # Batch size for processing
    batch_size = 100

    # Process Steam games in parallel
    print("Processing Steam games...")
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = []
        for i in range(0, len(steam_df), batch_size):
            batch = steam_df.iloc[i:i+batch_size]
            future = executor.submit(process_game_batch, batch, open_critic_df, wykonos_df, True)
            futures.append(future)
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Steam batches"):
            game_stats.extend(future.result())

    # Process Epic games in parallel
    print("Processing Epic games...")
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = []
        for i in range(0, len(epic_df), batch_size):
            batch = epic_df.iloc[i:i+batch_size]
            future = executor.submit(process_game_batch, batch, open_critic_df, wykonos_df, False)
            futures.append(future)
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Epic batches"):
            game_stats.extend(future.result())

    # Convert to DataFrame and save
    print("Saving dataset...")
    game_stats_df = pd.DataFrame(game_stats)
    
    # Vectorized operations for final cleanup
    game_stats_df['avg_critic_rating'] = np.where(
        game_stats_df['avg_critic_rating'] == 0,
        [generate_realistic_rating() for _ in range(len(game_stats_df))],
        game_stats_df['avg_critic_rating']
    )
    
    game_stats_df['avg_user_rating'] = np.where(
        game_stats_df['avg_user_rating'] == 0,
        [generate_realistic_rating() for _ in range(len(game_stats_df))],
        game_stats_df['avg_user_rating']
    )
    
    game_stats_df['completion_rate'] = game_stats_df['completion_rate'].fillna(0)
    
    # Save to CSV
    game_stats_df.to_csv('game-stats.csv', index=False)
    print("Dataset generated successfully!")

if __name__ == "__main__":
    main()