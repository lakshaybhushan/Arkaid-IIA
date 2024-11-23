import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import random
import faker
import pycountry

# Initialize Faker for generating realistic data
fake = faker.Faker()

def load_existing_games():
    """Load existing games from Steam and Epic CSV files"""
    try:
        steam_games = pd.read_csv('datasets/steam-games.csv')
        epic_games = pd.read_csv('datasets/epic-games.csv')
        return steam_games, epic_games
    except FileNotFoundError as e:
        print(f"Error loading game data: {e}")
        return None, None

def generate_publisher_data(steam_games, epic_games, num_rows=1000):
    """Generate publisher data based on existing games"""
    publishers = set()
    
    # Collect unique publishers from both platforms
    if 'Publishers' in steam_games.columns:
        publishers.update(steam_games['Publishers'].dropna().unique())
    if 'publisher' in epic_games.columns:
        publishers.update(epic_games['publisher'].dropna().unique())
    
    publishers = list(publishers)
    
    # Major gaming industry cities for headquarters
    major_cities = [
        "Tokyo, Japan", "San Francisco, USA", "Seattle, USA", "Los Angeles, USA",
        "Montreal, Canada", "London, UK", "Paris, France", "Berlin, Germany",
        "Seoul, South Korea", "Shanghai, China", "Singapore", "Warsaw, Poland",
        "Stockholm, Sweden", "Amsterdam, Netherlands", "Toronto, Canada",
        "New York, USA", "Boston, USA", "Kyoto, Japan", "Melbourne, Australia"
    ]
    
    publisher_data = []
    
    print("Generating publisher profiles...")
    for publisher in tqdm(publishers):
        # Get games published by this publisher
        steam_pub_games = steam_games[steam_games['Publishers'] == publisher]['Name'].tolist() if 'Publishers' in steam_games.columns else []
        epic_pub_games = epic_games[epic_games['publisher'] == publisher]['name'].tolist() if 'publisher' in epic_games.columns else []
        
        # Combine games from both platforms
        notable_games = list(set(steam_pub_games + epic_pub_games))
        
        # Generate establishment year between 1960 and 2015
        est_year = random.randint(1960, 2015)
        
        publisher_data.append({
            'Publisher': publisher,
            'Headquarters': random.choice(major_cities),
            'Est.': est_year,
            'Notable games published': ', '.join(notable_games[:5]) if notable_games else fake.sentence(),
            'Notes': fake.sentence() if random.random() > 0.7 else "",
            'Active': np.random.choice([0, 1], p=[0.1, 0.9])  # 90% chance of being active
        })
    
    # If we need more publishers to reach 1000, generate additional ones
    remaining_rows = num_rows - len(publisher_data)
    if remaining_rows > 0:
        print(f"Generating {remaining_rows} additional publisher profiles...")
        for _ in tqdm(range(remaining_rows)):
            publisher_name = f"{fake.company()} {random.choice(['Games', 'Entertainment', 'Interactive', 'Publishing', 'Media'])}".strip()
            publisher_data.append({
                'Publisher': publisher_name,
                'Headquarters': random.choice(major_cities),
                'Est.': random.randint(1960, 2015),
                'Notable games published': fake.sentence(),
                'Notes': fake.sentence() if random.random() > 0.7 else "",
                'Active': np.random.choice([0, 1], p=[0.1, 0.9])
            })
    
    return pd.DataFrame(publisher_data)

def main():
    # Load existing game data
    print("Loading existing game data...")
    steam_games, epic_games = load_existing_games()
    
    if steam_games is None or epic_games is None:
        print("Failed to load game data. Exiting...")
        return
    
    # Generate publisher data based on existing games
    print("\nGenerating publisher data...")
    publishers_df = generate_publisher_data(steam_games, epic_games, 1000)
    
    # Save to CSV file
    print("\nSaving publisher data...")
    publishers_df.to_csv('datasets/publisher.csv', index=False)
    
    print("Data generation complete!")
    print(f"Total publishers generated: {len(publishers_df)}")
    print(f"Unique publishers from Steam: {steam_games['Publishers'].nunique() if 'Publishers' in steam_games.columns else 0}")
    print(f"Unique publishers from Epic: {epic_games['publisher'].nunique() if 'publisher' in epic_games.columns else 0}")

if __name__ == "__main__":
    main() 