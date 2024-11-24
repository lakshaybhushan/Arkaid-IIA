import pandas as pd
import argparse
from thefuzz import fuzz
from thefuzz import process

def load_games():
    """Load the games dataset"""
    return pd.read_csv('dandan/fronkon_games.csv')

def search_game(df, query, threshold=60):
    """Search for games matching the query"""
    # Convert query to lowercase for case-insensitive search
    query = query.lower()
    
    # Create a list of all game names
    game_names = df['Name'].fillna('').tolist()
    
    # Find matches using fuzzy matching
    matches = process.extractBests(
        query, 
        game_names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
        limit=5
    )
    
    if not matches:
        print(f"\nNo games found matching '{query}'")
        return
    
    print(f"\nFound {len(matches)} matches for '{query}':")
    print("-" * 80)
    
    for game_name, score in matches:
        game_data = df[df['Name'] == game_name].iloc[0]
        
        # Print game details
        print(f"Game: {game_name}")
        print(f"Match Score: {score}%")
        print(f"Release Date: {game_data['Release date']}")
        print(f"Developer(s): {game_data['Developers']}")
        print(f"Publisher(s): {game_data['Publishers']}")
        print(f"Genres: {game_data['Genres']}")
        print(f"Price: ${game_data['Price']}")
        print("-" * 80)

def main():
    parser = argparse.ArgumentParser(description='Search for games in the Fronkon Games database')
    parser.add_argument('query', help='The game name to search for')
    parser.add_argument(
        '--threshold', 
        type=int, 
        default=60,
        help='Matching threshold (0-100). Default is 60'
    )
    
    args = parser.parse_args()
    
    try:
        df = load_games()
        search_game(df, args.query, args.threshold)
    except FileNotFoundError:
        print("Error: Could not find the games database file")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
