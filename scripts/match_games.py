import pandas as pd
from tqdm import tqdm

def clean_name(name):
    """Clean game names for exact matching"""
    if pd.isna(name):
        return ""
    name = str(name).lower()
    # Remove common suffixes/prefixes that might differ between datasets
    remove_terms = [
        "™", "®", "©", 
        "edition", "version", "game of the year",
        "goty", "remastered", "definitive", "complete",
        "collection", "bundle", "pack", "dlc"
    ]
    for term in remove_terms:
        name = name.replace(term, "")
    return name.strip()

def main():
    # Read the CSV files
    print("Reading CSV files...")
    try:
        wykonos_df = pd.read_csv('datasets/wykonos-games.csv')
        epic_df = pd.read_csv('datasets/epic-games.csv')
        
        # Clean names in both dataframes
        print("Cleaning game names...")
        wykonos_names = {clean_name(name) for name in wykonos_df['name'] if pd.notna(name)}
        epic_names = {clean_name(name) for name in epic_df['name'] if pd.notna(name)}
        
        # Find matches
        matching_names = wykonos_names.intersection(epic_names)
        
        # Calculate statistics
        total_wykonos = len(wykonos_names)
        total_epic = len(epic_names)
        total_matches = len(matching_names)
        
        # Print results
        print("\nMatching Statistics:")
        print(f"Total unique games in Wykonos dataset: {total_wykonos}")
        print(f"Total unique games in Epic dataset: {total_epic}")
        print(f"Games that exist in both datasets: {total_matches}")
        print(f"Match rate (based on Wykonos): {(total_matches/total_wykonos)*100:.2f}%")
        print(f"Match rate (based on Epic): {(total_matches/total_epic)*100:.2f}%")
        
        # Print some example matches (up to 10)
        print("\nExample matching games (up to 10):")
        for name in list(matching_names)[:10]:
            print(f"- {name}")
            
    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e}")
        print("Please ensure the following files exist:")
        print("- datasets/wykonos-games.csv")
        print("- datasets/epic-games.csv")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main() 