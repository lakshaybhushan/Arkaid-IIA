import pandas as pd
import numpy as np
from tqdm import tqdm
import random
import uuid

def load_existing_games():
    """Load existing games from Steam and Epic CSV files"""
    try:
        steam_games = pd.read_csv('datasets/steam-games.csv')
        epic_games = pd.read_csv('datasets/epic-games.csv')
        return steam_games, epic_games
    except FileNotFoundError as e:
        print(f"Error loading game data: {e}")
        return None, None

def generate_hardware_requirements(steam_games, epic_games):
    """Generate hardware requirements for existing games"""
    
    # Common hardware specifications
    os_options = [
        "Windows 10 64-bit",
        "Windows 8.1 64-bit",
        "Windows 7 64-bit",
        "Windows 11",
        "macOS 10.15+",
        "macOS 11.0+",
        "Ubuntu 20.04+",
    ]
    
    processor_options = [
        "Intel Core i3-3220 3.3 GHz",
        "Intel Core i5-2400 3.1 GHz",
        "Intel Core i5-6600K 3.5 GHz",
        "Intel Core i7-4770K 3.5 GHz",
        "AMD Ryzen 3 1200",
        "AMD Ryzen 5 1600",
        "AMD Ryzen 7 2700X",
        "AMD FX-6300",
    ]
    
    memory_options = [
        "4 GB RAM",
        "8 GB RAM",
        "12 GB RAM",
        "16 GB RAM",
        "32 GB RAM",
    ]
    
    graphics_options = [
        "NVIDIA GeForce GTX 660",
        "NVIDIA GeForce GTX 1060",
        "NVIDIA GeForce RTX 2060",
        "NVIDIA GeForce RTX 3060",
        "AMD Radeon RX 560",
        "AMD Radeon RX 580",
        "AMD Radeon RX 6600",
        "Intel UHD Graphics 630",
    ]
    
    storage_options = [
        "10 GB available space",
        "20 GB available space",
        "50 GB available space",
        "100 GB available space",
        "150 GB available space",
    ]
    
    hardware_data = []
    
    # Combine game IDs from both platforms
    game_ids = []
    if 'id' in steam_games.columns:
        game_ids.extend(steam_games['id'].tolist())
    if 'game_id' in epic_games.columns:
        game_ids.extend(epic_games['game_id'].tolist())
    
    print("Generating hardware requirements...")
    for game_id in tqdm(game_ids):
        # Generate hardware requirements for each game
        hardware_data.append({
            'hardware_id': str(uuid.uuid4()),
            'operacional_system': random.choice(os_options),
            'processor': random.choice(processor_options),
            'memory': random.choice(memory_options),
            'graphics': random.choice(graphics_options),
            'storage': random.choice(storage_options),
            'fk_game_id': str(game_id)
        })
    
    return pd.DataFrame(hardware_data)

def main():
    # Load existing game data
    print("Loading existing game data...")
    steam_games, epic_games = load_existing_games()
    
    if steam_games is None or epic_games is None:
        print("Failed to load game data. Exiting...")
        return
    
    # Generate hardware requirements data
    print("\nGenerating hardware requirements data...")
    hardware_df = generate_hardware_requirements(steam_games, epic_games)
    
    # Save to CSV file
    print("\nSaving hardware requirements data...")
    hardware_df.to_csv('datasets/necessary_hardware_filled.csv', index=False)
    
    print("Data generation complete!")
    print(f"Total hardware requirements generated: {len(hardware_df)}")

if __name__ == "__main__":
    main() 