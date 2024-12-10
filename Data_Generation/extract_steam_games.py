import pandas as pd
from tqdm import tqdm
import numpy as np

def extract_steam_games(input_file='steam_games.csv', epic_file='epic_games.csv', 
                       output_file='steam_games_1000.csv', n_rows=1000, n_matching=100):
    tqdm.pandas()
    
    dtype_dict = {
        'id': str,
        'Name': str,
        'Required age': np.int32,
        'Price': float,
        'DLC count': np.int32,
        'About the game': str,
        'Supported languages': str,
        'Full audio languages': str,
        'Reviews': str,
        'Header image': str,
        'Website': str,
        'Support url': str,
        'Support email': str,
        'Windows': bool,
        'Mac': bool,
        'Linux': bool,
        'Achievements': np.int32,
        'Recommendations': np.int32,
        'Notes': str,
        'Average playtime forever': np.int32,
        'Average playtime two weeks': np.int32,
        'Median playtime forever': np.int32,
        'Median playtime two weeks': np.int32,
        'Developers': str,
        'Publishers': str,
        'Categories': str,
        'Genres': str,
        'Tags': str,
        'Screenshots': str,
        'Movies': str
    }
    
    date_columns = ['Release date']
    
    try:
        print("Reading datasets...")
        # Read Steam Games dataset
        steam_df = pd.read_csv(
            input_file,
            dtype=dtype_dict,
            parse_dates=date_columns,
            on_bad_lines='warn'
        )
        
        # Read Epic Games dataset
        epic_df = pd.read_csv(epic_file)
        
        # Remove specified columns if they exist
        columns_to_remove = [
            'Score rank',
            'Positive',
            'Negative',
            'User score',
            'Metacritic score',
            'Metacritic url'
        ]
        
        # Drop columns that exist in the dataframe
        existing_columns = [col for col in columns_to_remove if col in steam_df.columns]
        if existing_columns:
            steam_df = steam_df.drop(columns=existing_columns)
        
        # Transform Required age column
        steam_df['Required age'] = steam_df['Required age'].apply(lambda x: 'Everyone (E)' if x == 0 else 'For Adult (A)')
        
        # Find matching games
        epic_games = set(epic_df['name'].str.lower().str.strip())
        steam_df['name_lower'] = steam_df['Name'].str.lower().str.strip()
        matching_mask = steam_df['name_lower'].isin(epic_games)
        
        # Get matching and non-matching games
        matching_games_df = steam_df[matching_mask]
        non_matching_games_df = steam_df[~matching_mask]
        
        # Sample from matching games
        n_matching = min(n_matching, len(matching_games_df))
        sampled_matching = matching_games_df.sample(n=n_matching, random_state=42)
        
        # Sample remaining rows from non-matching games
        n_remaining = n_rows - n_matching
        sampled_non_matching = non_matching_games_df.sample(n=n_remaining, random_state=42)
        
        # Combine the samples
        sampled_df = pd.concat([sampled_matching, sampled_non_matching])
        # Drop the temporary lowercase column
        sampled_df = sampled_df.drop(columns=['name_lower'])
        
        print(f"\nSaving {n_rows} rows to {output_file}...")
        print(f"Including {n_matching} matching games with Epic Games")
        # Save to CSV with progress bar
        sampled_df.to_csv(output_file, index=False)
        
        print("Extraction completed successfully!")
        return sampled_df
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    # Extract 1000 rows with 100 matching games
    extracted_df = extract_steam_games(n_rows=1000, n_matching=100)
    
    if extracted_df is not None:
        print("\nDataset Info:")
        print(f"Number of rows: {len(extracted_df)}")
        print(f"Number of columns: {len(extracted_df.columns)}")
        print("\nColumn names:")
        for col in extracted_df.columns:
            print(f"- {col}")