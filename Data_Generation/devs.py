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

def generate_developer_data(steam_games, epic_games, num_rows=1000):
    """Generate developer data based on existing games"""
    developers = set()
    
    # Collect unique developers from both platforms
    if 'Developers' in steam_games.columns:
        developers.update(steam_games['Developers'].dropna().unique())
    if 'developer' in epic_games.columns:
        developers.update(epic_games['developer'].dropna().unique())
    
    developers = list(developers)
    
    # Get list of countries and gaming cities
    countries = list(pycountry.countries)
    gaming_cities = [
        "Tokyo", "San Francisco", "Seattle", "Los Angeles", "Montreal", "Vancouver",
        "London", "Paris", "Berlin", "Stockholm", "Helsinki", "Seoul", "Singapore",
        "Warsaw", "Kyoto", "Amsterdam", "Toronto", "Melbourne", "Austin", "Boston"
    ]
    
    developer_data = []
    
    print("Generating developer profiles...")
    for developer in tqdm(developers):
        # Get games developed by this developer
        steam_dev_games = steam_games[steam_games['Developers'] == developer]['Name'].tolist() if 'Developers' in steam_games.columns else []
        epic_dev_games = epic_games[epic_games['developer'] == developer]['name'].tolist() if 'developer' in epic_games.columns else []
        
        # Combine games from both platforms
        notable_games = list(set(steam_dev_games + epic_dev_games))
        
        developer_data.append({
            'Developer': developer,
            'Active': np.random.choice([0, 1], p=[0.1, 0.9]),  # Fixed: Using np.random.choice
            'City': random.choice(gaming_cities),
            'Autonomous area': fake.state(),
            'Country': random.choice(countries).name,
            'Est.': fake.date_between(start_date='-50y', end_date='-1y'),
            'Notable games, series or franchises': ', '.join(notable_games[:5]) if notable_games else fake.sentence(),
            'Notes': fake.sentence() if random.random() > 0.7 else ""
        })
    
    # If we need more developers to reach 1000, generate additional ones
    remaining_rows = num_rows - len(developer_data)
    if remaining_rows > 0:
        print(f"Generating {remaining_rows} additional developer profiles...")
        for _ in tqdm(range(remaining_rows)):
            developer_name = f"{fake.company()} {random.choice(['Games', 'Studios', 'Entertainment', 'Interactive', 'Digital'])}".strip()
            developer_data.append({
                'Developer': developer_name,
                'Active': np.random.choice([0, 1], p=[0.1, 0.9]),  # Fixed: Using np.random.choice
                'City': random.choice(gaming_cities),
                'Autonomous area': fake.state(),
                'Country': random.choice(countries).name,
                'Est.': fake.date_between(start_date='-50y', end_date='-1y'),
                'Notable games, series or franchises': fake.sentence(),
                'Notes': fake.sentence() if random.random() > 0.7 else ""
            })
    
    return pd.DataFrame(developer_data)

def main():
    # Load existing game data
    print("Loading existing game data...")
    steam_games, epic_games = load_existing_games()
    
    if steam_games is None or epic_games is None:
        print("Failed to load game data. Exiting...")
        return
    
    # Generate developer data based on existing games
    print("\nGenerating developer data...")
    developers_df = generate_developer_data(steam_games, epic_games, 1000)
    
    # Save to CSV file
    print("\nSaving developer data...")
    developers_df.to_csv('newdata/developper.csv', index=False)
    
    print("Data generation complete!")
    print(f"Total developers generated: {len(developers_df)}")
    print(f"Unique developers from Steam: {steam_games['Developers'].nunique() if 'Developers' in steam_games.columns else 0}")
    print(f"Unique developers from Epic: {epic_games['developer'].nunique() if 'developer' in epic_games.columns else 0}")

if __name__ == "__main__":
    main()
