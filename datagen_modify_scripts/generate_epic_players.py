import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime, timedelta, date
import uuid

# Read epic games data
epic_games_df = pd.read_csv('new/epic_games.csv')
game_ids = epic_games_df['game_id'].tolist()

# Dictionary mapping countries to their locale codes
COUNTRY_LOCALES = {
    'United States': 'en_US',
    'United Kingdom': 'en_GB',
    'Canada': 'en_CA',
    'Australia': 'en_AU',
    'India': 'en_IN',
    'Germany': 'de_DE',
    'France': 'fr_FR',
    'Spain': 'es_ES',
    'Japan': 'ja_JP',
    'Brazil': 'pt_BR',
    'Mexico': 'es_MX',
    'Netherlands': 'nl_NL',
    'Sweden': 'sv_SE',
    'Italy': 'it_IT',
    'South Korea': 'ko_KR'
}

# Payment methods
PAYMENT_METHODS = ['PayPal', 'Credit Card', 'Debit Card', 'Apple Pay', 
                  'Google Pay', 'Bank Transfer', 'Epic Wallet']

# Initialize country-specific fakers
fakers = {country: Faker([locale]) for country, locale in COUNTRY_LOCALES.items()}

def generate_username(name):
    """Generate username based on name"""
    name = name.lower().replace(' ', '')
    random_num = random.randint(1, 999)
    return f"{name}{random_num}"

def generate_email(name, username):
    """Generate email based on name and username"""
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com']
    domain = random.choice(domains)
    return f"{username}@{domain}"

def generate_library():
    """Generate random game library"""
    num_games = random.randint(1, 10)
    return random.sample(game_ids, num_games)

def generate_favorites(library):
    """Generate favorites from library"""
    num_favorites = min(len(library), random.randint(1, 3))
    return random.sample(library, num_favorites)

def generate_dob():
    """Generate date of birth with higher probability for ages 16-30"""
    today = date.today()
    
    # Age distribution weights
    age_ranges = {
        (16, 20): 0.30,  # 30% probability
        (21, 25): 0.35,  # 35% probability
        (26, 30): 0.20,  # 20% probability
        (31, 40): 0.10,  # 10% probability
        (41, 50): 0.05   # 5% probability
    }
    
    # Select age range based on weights
    age_range = random.choices(list(age_ranges.keys()), 
                             weights=list(age_ranges.values()))[0]
    
    # Generate random age within selected range
    age = random.randint(age_range[0], age_range[1])
    
    # Calculate birth date
    birth_year = today.year - age
    birth_month = random.randint(1, 12)
    birth_day = random.randint(1, 28)  # Using 28 to avoid invalid dates
    
    return date(birth_year, birth_month, birth_day)

def generate_player_data(num_players=4000):
    data = []
    
    for _ in tqdm(range(num_players), desc="Generating player data"):
        # Select random country and its faker
        country = random.choice(list(COUNTRY_LOCALES.keys()))
        fake = fakers[country]
        
        # Generate basic info
        name = fake.name()
        username = generate_username(name)
        
        # Generate date of birth with age distribution
        dob = generate_dob()
        age = (date.today() - dob).days // 365
        
        # Generate library and favorites
        library = generate_library()
        favorites = generate_favorites(library)
        
        # Generate other fields
        player = {
            'id': str(uuid.uuid4())[:20],
            'name': name,
            'username': username,
            'age': age,
            'dob': dob.strftime('%Y-%m-%d'),
            'email': generate_email(name, username),
            'phone': fake.phone_number().replace('-', '').replace(' ', '')[:15],
            'library': ','.join(map(str, library)),
            'favorites': ','.join(map(str, favorites)),
            'payment_method': random.choice(PAYMENT_METHODS),
            'country': country,
            'total_hrs_played': round(random.uniform(1, 1000), 2),
            'avg_rating_given': round(random.uniform(50, 100), 1)
        }
        
        data.append(player)
    
    return data

def main():
    # Generate player data
    players_data = generate_player_data(4000)
    
    # Convert to DataFrame
    df = pd.DataFrame(players_data)
    
    # Save to CSV
    df.to_csv('new/epic_players.csv', index=False)
    print("\nData generation complete! File saved as 'new/epic_players.csv'")
    
    # Print age distribution statistics
    ages = df['age'].values
    print("\nAge Distribution Statistics:")
    print(f"Mean Age: {np.mean(ages):.2f}")
    print(f"Median Age: {np.median(ages):.2f}")
    print("\nAge Group Distribution:")
    print(df['age'].value_counts(bins=[15,20,25,30,35,40,45,50], normalize=True).sort_index() * 100)

if __name__ == "__main__":
    main() 