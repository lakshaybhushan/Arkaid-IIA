import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
import random
import string

def generate_alphanumeric_id(length=32):
    """Generate an alphanumeric ID similar to game IDs"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def romanize_name(name, country):
    """Convert names to Latin alphabet based on country"""
    if country == 'Japan':
        surnames = ['Sato', 'Suzuki', 'Takahashi', 'Tanaka', 'Watanabe', 'Ito', 'Yamamoto', 
                   'Nakamura', 'Kobayashi', 'Saito', 'Kato', 'Yoshida', 'Yamada', 'Sasaki',
                   'Yamaguchi', 'Matsumoto', 'Inoue', 'Kimura', 'Hayashi', 'Shimizu']
        given_names = ['Hiroshi', 'Takashi', 'Kenji', 'Yuki', 'Akira', 'Yuko', 'Sakura',
                      'Kaito', 'Haruto', 'Yuta', 'Sota', 'Rin', 'Hana', 'Yui', 'Aoi',
                      'Riku', 'Mei', 'Sora', 'Kenta', 'Naoki', 'Yuna', 'Emi', 'Kei']
        return f"{random.choice(surnames)} {random.choice(given_names)}"
    
    elif country == 'South Korea':
        surnames = ['Kim', 'Lee', 'Park', 'Choi', 'Jung', 'Kang', 'Cho', 
                   'Yoon', 'Jang', 'Lim', 'Han', 'Oh', 'Seo', 'Shin', 
                   'Yang', 'Bae', 'Hwang', 'Song', 'Yoo', 'Hong']
        given_names = ['Min-jun', 'Seo-yeon', 'Ji-woo', 'Min-seo', 'Jun-ho',
                      'Seo-jun', 'Do-yeon', 'Ye-jun', 'Ha-eun', 'Ji-min',
                      'Dong-hyun', 'Joo-won', 'Seung-min', 'Hye-jin', 'Yu-na',
                      'Min-ho', 'Soo-jin', 'Ji-hoon', 'Eun-ji', 'Tae-hyung']
        return f"{random.choice(surnames)} {random.choice(given_names)}"
    
    return name

def generate_modders():
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    Faker.seed(42)
    
    # Define mod types with descriptions
    mod_types = {
        'Gameplay Overhaul': 0.15,    # Complete game mechanics changes
        'Graphics Enhancement': 0.20,  # Visual improvements and textures
        'New Content': 0.25,          # New items, quests, or areas
        'Quality of Life': 0.15,      # UI improvements and convenience features
        'Bug Fix': 0.10,              # Community patches and fixes
        'Balance Adjustment': 0.05,    # Game balance modifications
        'Translation': 0.05,          # Language additions
        'Sound Overhaul': 0.05        # Audio modifications
    }

    # Define countries and their weights
    countries = {
        'United States': 0.25,
        'United Kingdom': 0.12,
        'Germany': 0.08,
        'Japan': 0.08,
        'Russia': 0.07,
        'France': 0.06,
        'South Korea': 0.06,
        'Brazil': 0.05,
        'Poland': 0.05,
        'Australia': 0.05,
        'Sweden': 0.04,
        'Netherlands': 0.03,
        'China': 0.03,
        'Italy': 0.03
    }

    # Initialize Faker for different locales
    fake_by_country = {
        'United States': Faker('en_US'),
        'United Kingdom': Faker('en_GB'),
        'Germany': Faker('de_DE'),
        'Japan': Faker('en_US'),
        'Russia': Faker('ru_RU'),
        'France': Faker('fr_FR'),
        'South Korea': Faker('en_US'),
        'Brazil': Faker('pt_BR'),
        'Poland': Faker('pl_PL'),
        'Australia': Faker('en_AU'),
        'Sweden': Faker('sv_SE'),
        'Netherlands': Faker('nl_NL'),
        'China': Faker('en_US'),
        'Italy': Faker('it_IT')
    }

    # Read the game datasets
    steam_df = pd.read_csv('datasets/steam-games.csv')
    epic_df = pd.read_csv('datasets/epic-games.csv')

    # Prepare game IDs and their weights based on estimated owners
    steam_games = steam_df[['id', 'Estimated owners']].copy()
    steam_games['Estimated owners'] = steam_games['Estimated owners'].fillna('0-0')
    steam_games['weight'] = steam_games['Estimated owners'].apply(
        lambda x: float(x.split('-')[1].strip()) if isinstance(x, str) else 0
    )

    epic_games = epic_df[['game_id']].copy()
    epic_games['weight'] = 100000  # Default weight for Epic games

    # Combine game IDs from both platforms
    all_games = pd.concat([
        steam_games[['id', 'weight']].rename(columns={'id': 'game_id'}),
        epic_games
    ])

    # Normalize weights
    all_games['weight'] = all_games['weight'] / all_games['weight'].sum()

    # Generate modders
    num_modders = 2500  # Number of modders to generate
    modders_data = []

    for _ in tqdm(range(num_modders), desc="Generating modders"):
        # Generate modder ID
        modder_id = generate_alphanumeric_id()

        # Select country based on weights
        country = np.random.choice(
            list(countries.keys()),
            p=list(countries.values())
        )

        # Generate name based on country and romanize if needed
        name = fake_by_country[country].name()
        name = romanize_name(name, country)

        # Select primary game based on weights
        primary_game = np.random.choice(all_games['game_id'], p=all_games['weight'])

        # Select mod type based on weights
        mod_type = np.random.choice(
            list(mod_types.keys()),
            p=list(mod_types.values())
        )

        modders_data.append({
            'ID': modder_id,
            'Name': name,
            'Primary_Game_ID': primary_game,
            'Type_of_Mod': mod_type,
            'Country': country
        })

    # Create DataFrame and save to CSV
    modders_df = pd.DataFrame(modders_data)
    modders_df.to_csv('datasets/modders.csv', index=False)
    print(f"Generated {num_modders} modders and saved to modders.csv")

    # Display sample of the generated data and distribution info
    print("\nSample of generated data:")
    print(modders_df.head())
    
    print("\nDistribution of countries:")
    print(modders_df['Country'].value_counts(normalize=True))
    
    print("\nDistribution of mod types:")
    print(modders_df['Type_of_Mod'].value_counts(normalize=True))

if __name__ == "__main__":
    generate_modders() 