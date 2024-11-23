import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
from tqdm import tqdm
import uuid
import re

# Initialize Faker with a seed for consistency
fake = Faker()

def generate_username_from_name(name):
    """Generate a username based on the person's name"""
    # Remove any special characters and convert to lowercase
    clean_name = re.sub(r'[^a-zA-Z\s]', '', name.lower())
    first_name, last_name = clean_name.split(' ')[0:2]
    
    username_styles = [
        lambda f, l: f"{f}{l}",                          # johnsmith
        lambda f, l: f"{f[0]}{l}",                       # jsmith
        lambda f, l: f"{f}{l[0]}",                       # johns
        lambda f, l: f"{f}_{l}",                         # john_smith
        lambda f, l: f"{f}.{l}",                         # john.smith
        lambda f, l: f"{f}{random.randint(1,999)}",      # john123
        lambda f, l: f"{f[0]}{l}{random.randint(1,99)}"  # jsmith42
    ]
    
    username_style = random.choice(username_styles)
    username = username_style(first_name, last_name)
    
    # Add random numbers if we want to ensure uniqueness
    if random.random() < 0.3:  # 30% chance to add random numbers
        username += str(random.randint(1, 9999))
        
    return username

def generate_players_data():
    print("Loading data files...")
    # Load files once and convert relevant columns to lists/sets for faster operations
    player_locations = pd.read_csv('datasets/player_locations.csv')
    player_stats = pd.read_csv('newdata/player_stats.csv')
    player_sales = pd.read_csv('datasets/player_sales.csv')
    steam_games = pd.read_csv('datasets/steam-games.csv')
    epic_games = pd.read_csv('datasets/epic-games.csv')

    # Pre-process data for faster lookups
    all_games = set(steam_games['id'].tolist() + epic_games['game_id'].tolist())
    
    # Create dictionaries for faster lookups
    stats_by_player = player_stats.groupby('playerId')
    sales_by_player = player_sales.groupby('PlayerID')['SaleID'].agg(list).to_dict()
    
    # Payment methods available
    payment_methods = ['Credit Card', 'Debit Card', 'NetBanking', 'PayPal', 'ApplePay', 'GooglePay']
    
    # Pre-calculate date ranges for better performance
    today = datetime.now().date()
    
    def generate_single_player(row):
        player_id = row['PlayerID']
        
        # Get player's game stats efficiently
        try:
            player_games = stats_by_player.get_group(player_id)
            all_games_owned = player_games['fk_gameid'].tolist()
            total_hours = player_games['hrs_played'].sum()
            favorites = player_games[player_games['favorites'] == 1]['fk_gameid'].tolist()
            recently_played = player_games['fk_gameid'].iloc[-1] if not player_games.empty else None
        except KeyError:
            all_games_owned = []
            total_hours = 0
            favorites = []
            recently_played = None
        
        # Generate basic info
        if random.random() < 0.7:
            dob = fake.date_of_birth(minimum_age=18, maximum_age=30)
        else:
            dob = fake.date_of_birth(minimum_age=12, maximum_age=80)
        age = (today - dob).days // 365
        
        # Generate name first, then username based on name
        name = fake.name()
        username = generate_username_from_name(name)
        
        # Select random 10-15% games as currently playing
        num_playing = int(len(all_games_owned) * random.uniform(0.1, 0.15))
        playing = random.sample(all_games_owned, min(num_playing, len(all_games_owned)))
        
        # Generate wishlist and cart more efficiently
        available_games = list(all_games - set(all_games_owned))
        wishlist_size = random.randint(0, 20)
        wishlist = random.sample(available_games, min(wishlist_size, len(available_games)))
        
        # Cart generation
        cart = []
        if random.random() > 0.7:
            cart_size = random.randint(1, 3)
            cart_pool = available_games + wishlist
            cart = random.sample(cart_pool, min(cart_size, len(cart_pool)))
        
        return {
            'ID': player_id,
            'Name': name,  # Name comes before username in the dictionary
            'Username': username,
            'Age': age,
            'DOB': dob.strftime('%Y-%m-%d'),
            'RecentlyPlayed': recently_played,
            'Email': f"{username.lower()}@{fake.free_email_domain()}",
            'Phone': fake.phone_number(),
            'List_of_SaleIDs': sales_by_player.get(player_id, []),
            'Total_Hours_Played': round(total_hours, 2),
            'Library_All': all_games_owned,
            'Library_Favorites': favorites,
            'Library_Playing': playing,
            'Library_Title': all_games_owned,
            'Wishlist': wishlist,
            'Cart': cart,
            'PaymentMethod': random.sample(payment_methods, random.randint(1, 2)),
            'Wallet': round(random.uniform(0, 500), 2),
            'Country': row['Country']
        }

    print("Generating player data...")
    # Use list comprehension instead of appending in a loop
    players_data = [generate_single_player(row) for _, row in tqdm(player_locations.iterrows(), 
                                                                  total=len(player_locations), 
                                                                  desc="Generating Players")]
    
    print("Converting to DataFrame and saving...")
    df = pd.DataFrame(players_data)
    df.to_csv('newdata/players.csv', index=False)
    print("Players data generated successfully!")

if __name__ == "__main__":
    generate_players_data()