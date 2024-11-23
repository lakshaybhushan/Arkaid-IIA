import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
import uuid
import random
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

def load_game_data():
    """Load and combine game data from Steam and Epic with weighted popularity"""
    # Load Steam games
    steam_df = pd.read_csv('datasets/steam-games.csv')
    steam_df['platform'] = 'Steam'
    
    # Calculate popularity weight for Steam games based on playtime
    max_playtime = steam_df['Average playtime forever'].max()
    steam_df['popularity_weight'] = (steam_df['Average playtime forever'] / max_playtime * 10).fillna(1)
    # Ensure minimum weight of 1
    steam_df['popularity_weight'] = steam_df['popularity_weight'].clip(lower=1)
    
    steam_df = steam_df[['id', 'Name', 'Price', 'platform', 'popularity_weight']]
    steam_df.columns = ['game_id', 'name', 'price', 'platform', 'popularity_weight']
    
    # Load Epic games
    epic_df = pd.read_csv('datasets/epic-games.csv')
    epic_df = epic_df[['game_id', 'name', 'price', 'platform']]
    # Set default popularity weight for Epic games
    epic_df['popularity_weight'] = 1
    
    # Combine datasets
    games_df = pd.concat([steam_df, epic_df], ignore_index=True)
    # Clean price data
    games_df['price'] = pd.to_numeric(games_df['price'], errors='coerce')
    games_df = games_df.dropna(subset=['price'])
    return games_df

def generate_player_sales(num_players=40000, min_games_per_player=1, max_games_per_player=200):
    """Generate synthetic player sales data"""
    
    fake = Faker()
    Faker.seed(42)
    
    # Load game data
    games_df = load_game_data()
    
    # Lists to store our generated data
    sales_data = []
    player_data = []
    
    # Track player-game combinations to prevent duplicates
    player_game_combinations = set()
    
    # Track game purchase counts to ensure each game has at least one purchase
    game_purchase_counts = {game_id: 0 for game_id in games_df['game_id']}
    
    print("Generating player sales data...")
    for _ in tqdm(range(num_players)):
        # Generate player data
        player_id = str(uuid.uuid4())
        home_country = fake.country()
        player_data.append({
            'PlayerID': player_id,
            'Country': home_country
        })
        
        # Random number of games for this player
        num_games = random.randint(min_games_per_player, max_games_per_player)
        
        # Select games based on popularity weight
        available_games = games_df.sample(
            n=min(num_games, len(games_df)),
            weights='popularity_weight',
            replace=False
        )
        
        for _, game in available_games.iterrows():
            game_id = game['game_id']
            
            # Check if this player-game combination already exists
            if (player_id, game_id) in player_game_combinations:
                continue
                
            player_game_combinations.add((player_id, game_id))
            game_purchase_counts[game_id] += 1
            
            # Get game price directly from the dataframe
            game_price = float(game['price'])
            
            # Determine sale location (80% chance of home country)
            if random.random() < 0.8:
                sale_country = home_country
                fake.seed_instance(hash(home_country + str(_)))
                sale_city = fake.city()
            else:
                sale_country = fake.country()
                while sale_country == home_country:
                    sale_country = fake.country()
                fake.seed_instance(hash(sale_country + str(_)))
                sale_city = fake.city()
            
            sales_data.append({
                'SaleID': str(uuid.uuid4()),
                'GameID': game_id,
                'PlayerID': player_id,
                'Price': game_price,
                'CountrySold': sale_country,
                'LocationSold': sale_city
            })
    
    # Ensure each game has at least one purchase
    unpurchased_games = [gid for gid, count in game_purchase_counts.items() if count == 0]
    if unpurchased_games:
        print(f"\nAdding purchases for {len(unpurchased_games)} unpurchased games...")
        for game_id in unpurchased_games:
            game_data = games_df[games_df['game_id'] == game_id].iloc[0]
            player_id = str(uuid.uuid4())
            home_country = fake.country()
            
            # Add new player
            player_data.append({
                'PlayerID': player_id,
                'Country': home_country
            })
            
            # Add sale for unpurchased game
            sales_data.append({
                'SaleID': str(uuid.uuid4()),
                'GameID': game_id,
                'PlayerID': player_id,
                'Price': float(game_data['price']),
                'CountrySold': home_country,
                'LocationSold': fake.city()
            })
    
    # Create DataFrames
    sales_df = pd.DataFrame(sales_data)
    player_df = pd.DataFrame(player_data)
    
    # Save to CSV
    sales_df.to_csv('datasets/player_sales.csv', index=False)
    player_df.to_csv('datasets/player_locations.csv', index=False)
    
    print(f"Generated {len(sales_df)} sales records for {len(player_df)} players")
    
    # Print some statistics
    print("\nData Statistics:")
    print(f"Total unique players: {len(sales_df['PlayerID'].unique())}")
    print(f"Total unique games sold: {len(sales_df['GameID'].unique())}")
    print(f"Average games per player: {len(sales_df) / len(player_df):.2f}")
    print(f"Average price per sale: ${sales_df['Price'].mean():.2f}")
    
    # Game purchase distribution statistics
    purchase_counts = sales_df['GameID'].value_counts()
    print("\nGame Purchase Statistics:")
    print(f"Average purchases per game: {purchase_counts.mean():.2f}")
    print(f"Median purchases per game: {purchase_counts.median():.2f}")
    print(f"Max purchases for a game: {purchase_counts.max()}")
    print(f"Min purchases for a game: {purchase_counts.min()}")
    
    # Calculate and print location statistics
    sales_locations = sales_df.groupby('PlayerID').agg({
        'CountrySold': 'nunique'
    }).describe()
    
    print("\nCountries per player statistics:")
    print(f"Average number of different countries per player: {sales_locations['CountrySold']['mean']:.2f}")
    print(f"Max number of different countries per player: {sales_locations['CountrySold']['max']:.0f}")
    
if __name__ == "__main__":
    generate_player_sales(num_players=40000, min_games_per_player=1, max_games_per_player=200)