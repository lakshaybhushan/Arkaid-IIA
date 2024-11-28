import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime, timedelta
import uuid

# Read steam games data
steam_games = pd.read_csv('new/steam_games.csv')
game_names = steam_games['Name'].dropna().tolist()

# List of countries and their locales
COUNTRY_LOCALES = {
    'United States': 'en_US',
    'United Kingdom': 'en_GB',
    'Canada': 'en_CA',
    'Australia': 'en_AU',
    'Germany': 'de_DE',
    'France': 'fr_FR',
    'Spain': 'es_ES',
    'Italy': 'it_IT',
    'Japan': 'ja_JP',
    'China': 'zh_CN',
    'India': 'en_IN',
    'Brazil': 'pt_BR',
    'Mexico': 'es_MX',
    'Russia': 'ru_RU',
    'Netherlands': 'nl_NL'
}

PAYMENT_METHODS = ['Apple Pay', 'PayPal', 'Credit Card', 'Debit Card', 'Google Pay', 'Bank Transfer']

def generate_username(name):
    """Generate username based on name"""
    name = name.lower().replace(' ', '')
    random_num = random.randint(100, 999)
    return f"{name}{random_num}"

def generate_player_data(faker, country):
    """Generate a single player's data"""
    # Generate basic info
    name = faker.name()
    username = generate_username(name)
    
    # Generate date of birth with weighted age distribution
    today = datetime.now()
    # Weighted age distribution favoring 16-30 range
    age_weights = {
        range(13, 16): 0.1,  # 10% chance
        range(16, 24): 0.4,  # 40% chance
        range(24, 31): 0.3,  # 30% chance
        range(31, 41): 0.15, # 15% chance
        range(41, 71): 0.05  # 5% chance
    }
    
    # Select age range based on weights
    age_range = random.choices(list(age_weights.keys()), 
                             weights=list(age_weights.values()))[0]
    age = random.choice(list(age_range))
    dob = today - timedelta(days=age*365 + random.randint(0, 365))
    
    # Generate email based on username
    email_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
    email = f"{username}@{random.choice(email_domains)}"
    
    # Generate phone number
    phone = faker.phone_number()
    
    # Generate game library and favorites
    num_games = random.randint(1, 10)
    library = random.sample(game_names, num_games)
    num_favorites = min(random.randint(1, 3), len(library))
    favorites = random.sample(library, num_favorites)
    
    # Generate other data
    payment_method = random.choice(PAYMENT_METHODS)
    total_hrs_played = round(random.uniform(1, 1000), 2)
    avg_rating = round(random.uniform(50, 100), 1)
    
    return {
        'id': str(uuid.uuid4().hex),
        'name': name,
        'username': username,
        'age': age,
        'dob': dob.strftime('%Y-%m-%d'),
        'email': email,
        'phone': phone.replace('-', '').replace(' ', '').replace('(', '').replace(')', ''),
        'library': ', '.join(library),
        'favorites': ', '.join(favorites),
        'payment_method': payment_method,
        'country': country,
        'total_hrs_played': total_hrs_played,
        'avg_rating_given': avg_rating
    }

def main():
    # Increased number of players (random between 4000-5000)
    num_players = random.randint(4000, 5000)
    
    # Initialize list to store player data
    players_data = []
    
    # Create progress bar
    pbar = tqdm(total=num_players, desc="Generating player data")
    
    # Generate players
    for _ in range(num_players):
        # Randomly select a country and its locale
        country = random.choice(list(COUNTRY_LOCALES.keys()))
        fake = Faker(COUNTRY_LOCALES[country])
        
        # Generate player data
        player = generate_player_data(fake, country)
        players_data.append(player)
        
        pbar.update(1)
    
    pbar.close()
    
    # Convert to DataFrame and save
    df = pd.DataFrame(players_data)
    df.to_csv('new/steam_players.csv', index=False)
    print(f"Generated {num_players} players data and saved to steam_players.csv")

if __name__ == "__main__":
    main() 