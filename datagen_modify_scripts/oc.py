import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import random
import faker
import uuid

# Initialize Faker for generating realistic data
fake = faker.Faker()

def load_existing_data():
    """Load existing games and critic data"""
    try:
        steam_games = pd.read_csv('datasets/steam-games.csv')
        epic_games = pd.read_csv('datasets/epic-games.csv')
        try:
            existing_critics = pd.read_csv('datasets/open_critic.csv')
        except FileNotFoundError:
            existing_critics = pd.DataFrame(columns=['id', 'company', 'author', 'rating', 'comment', 'date', 'top_critic', 'game_id'])
        return steam_games, epic_games, existing_critics
    except FileNotFoundError as e:
        print(f"Error loading data: {e}")
        return None, None, None

def generate_realistic_comment(rating, game_name="", genre=""):
    """Generate more realistic review comments based on rating ranges"""
    
    # Comment templates based on rating ranges
    high_rating_templates = [
        "An absolute masterpiece that sets new standards for the {genre} genre. {game} delivers an unforgettable experience with its {pos_aspect} and {pos_aspect}. {conclusion}",
        "In a league of its own, {game} brilliantly combines {pos_aspect} with {pos_aspect}, creating one of the most {pos_adj} experiences of the year. {conclusion}",
        "{game} is a triumph of {genre} game design, featuring {pos_aspect} that will keep players engaged for hours. {conclusion}",
        "Exceptional in every way, {game} showcases {pos_aspect} while maintaining {pos_aspect}. {conclusion}"
    ]
    
    mid_rating_templates = [
        "While {game} shows promise with its {pos_aspect}, it struggles with {neg_aspect}. {conclusion}",
        "A solid {genre} title that delivers {pos_aspect}, though {neg_aspect} holds it back from greatness. {conclusion}",
        "{game} offers {pos_aspect}, but {neg_aspect} might deter some players. {conclusion}",
        "Despite {neg_aspect}, {game} manages to provide {pos_aspect}. {conclusion}"
    ]
    
    low_rating_templates = [
        "Unfortunately, {game} falls short due to {neg_aspect} and {neg_aspect}. {conclusion}",
        "A disappointing entry in the {genre} genre, suffering from {neg_aspect} throughout. {conclusion}",
        "{game} struggles to deliver on its promises, with {neg_aspect} being particularly problematic. {conclusion}",
        "Marred by {neg_aspect}, {game} fails to capture what makes {genre} games engaging. {conclusion}"
    ]
    
    positive_aspects = [
        "stunning visuals", "engaging storytelling", "innovative gameplay mechanics",
        "memorable character development", "atmospheric world design", "compelling narrative",
        "polished combat system", "immersive sound design", "strategic depth",
        "seamless multiplayer integration", "rich environmental storytelling"
    ]
    
    negative_aspects = [
        "technical issues", "repetitive gameplay loops", "inconsistent performance",
        "shallow character development", "dated graphics", "clunky controls",
        "poor optimization", "lack of content", "uninspired level design",
        "frustrating difficulty spikes", "questionable design choices"
    ]
    
    positive_adjectives = [
        "compelling", "innovative", "polished", "refined", "engaging",
        "immersive", "memorable", "outstanding", "remarkable", "captivating"
    ]
    
    conclusions = [
        "A must-play for fans of the genre.",
        "Worth every moment spent playing.",
        "Sets a new benchmark for future titles.",
        "Players will find themselves thoroughly entertained.",
        "The experience leaves a lasting impression.",
        "Requires significant improvements to reach its potential.",
        "Needs more polish to justify the investment.",
        "May appeal to die-hard fans, but others should wait for updates.",
        "Difficult to recommend in its current state."
    ]
    
    if rating >= 80:
        template = random.choice(high_rating_templates)
        conclusion = random.choice(conclusions[:5])
    elif rating >= 60:
        template = random.choice(mid_rating_templates)
        conclusion = random.choice(conclusions[3:7])
    else:
        template = random.choice(low_rating_templates)
        conclusion = random.choice(conclusions[5:])
    
    return template.format(
        game=game_name if game_name else "the game",
        genre=genre if genre else "gaming",
        pos_aspect=random.choice(positive_aspects),
        pos_adj=random.choice(positive_adjectives),
        neg_aspect=random.choice(negative_aspects),
        conclusion=conclusion
    )

def generate_additional_critic_data(steam_games, epic_games, existing_critics, num_additional_reviews=1000):
    """Generate additional critic reviews data based on existing games"""
    
    review_companies = [
        "IGN", "GameSpot", "Polygon", "Eurogamer", "PC Gamer",
        "Game Informer", "Kotaku", "Rock Paper Shotgun", "Destructoid",
        "Giant Bomb", "Easy Allies", "VG247", "GamesRadar+", "VentureBeat"
    ]
    
    critic_data = []
    existing_combinations = set()
    
    if not existing_critics.empty:
        existing_combinations = set(zip(existing_critics['author'], existing_critics['game_id']))
    
    # Combine game IDs and names from both platforms
    game_info = []
    if 'id' in steam_games.columns and 'Genres' in steam_games.columns:
        game_info.extend(zip(steam_games['id'], steam_games['Name'], steam_games['Genres']))
    if 'game_id' in epic_games.columns and 'genres' in epic_games.columns:
        game_info.extend(zip(epic_games['game_id'], epic_games['name'], epic_games['genres']))
    
    print("Generating additional critic reviews...")
    with tqdm(total=num_additional_reviews) as pbar:
        while len(critic_data) < num_additional_reviews:
            game_id, game_name, genre = random.choice(game_info)
            company = random.choice(review_companies)
            author = f"{fake.first_name()} {fake.last_name()}"
            
            if (author, str(game_id)) in existing_combinations:
                continue
            
            # Generate rating between 10 and 100
            rating = random.randint(10, 100)
            
            # Generate review date within last 2 years
            date = fake.date_between(start_date='-2y', end_date='now')
            
            # Generate realistic comment
            comment = generate_realistic_comment(rating, game_name, genre.split(',')[0] if isinstance(genre, str) else "")
            
            critic_data.append({
                'id': str(uuid.uuid4()),
                'company': company,
                'author': author,
                'rating': rating,
                'comment': comment,
                'date': date,
                'top_critic': np.random.choice([True, False], p=[0.2, 0.8]),
                'game_id': str(game_id)
            })
            
            existing_combinations.add((author, str(game_id)))
            pbar.update(1)
    
    new_reviews_df = pd.DataFrame(critic_data)
    
    if not existing_critics.empty:
        combined_df = pd.concat([existing_critics, new_reviews_df], ignore_index=True)
    else:
        combined_df = new_reviews_df
    
    return combined_df

def main():
    print("Loading existing data...")
    steam_games, epic_games, existing_critics = load_existing_data()
    
    if steam_games is None or epic_games is None:
        print("Failed to load game data. Exiting...")
        return
    
    print("\nGenerating additional critic reviews...")
    combined_critics_df = generate_additional_critic_data(steam_games, epic_games, existing_critics, 1000)
    
    print("\nSaving updated critic review data...")
    combined_critics_df.to_csv('datasets/open_critic.csv', index=False)
    
    print("\nData generation complete!")
    print(f"Previous number of reviews: {len(existing_critics) if not existing_critics.empty else 0}")
    print(f"New total reviews: {len(combined_critics_df)}")
    print(f"Unique games reviewed: {combined_critics_df['game_id'].nunique()}")
    print(f"Unique critics: {combined_critics_df['author'].nunique()}")
    print(f"Average rating: {combined_critics_df['rating'].mean():.2f}")
    print(f"Percentage of top critics: {(combined_critics_df['top_critic'].sum() / len(combined_critics_df) * 100):.1f}%")

if __name__ == "__main__":
    main()