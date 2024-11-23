import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from tqdm import tqdm
import re
import random

# Read the datasets
print("Loading datasets...")
hardware_df = pd.read_csv('datasets/necessary_hardware.csv')
epic_games_df = pd.read_csv('datasets/epic-games.csv')

# Function to generate processor requirements
def generate_processor_req():
    processors = [
        'Intel Core i3', 'Intel Core i5', 'Intel Core i7', 'Intel Core i9',
        'AMD Ryzen 3', 'AMD Ryzen 5', 'AMD Ryzen 7', 'AMD Ryzen 9'
    ]
    generations = range(3, 13)  # Extended range for more variety
    speeds = [f"{speed:.1f} GHz" for speed in np.arange(2.0, 5.1, 0.2)]  # More speed options
    
    return f"{np.random.choice(processors)} {np.random.choice(generations)}000 {np.random.choice(speeds)}"

# Function to generate graphics requirements
def generate_graphics_req():
    nvidia_gpus = [
        'NVIDIA GTX 1050', 'NVIDIA GTX 1060', 'NVIDIA GTX 1070', 'NVIDIA GTX 1080',
        'NVIDIA RTX 2060', 'NVIDIA RTX 2070', 'NVIDIA RTX 2080',
        'NVIDIA RTX 3060', 'NVIDIA RTX 3070', 'NVIDIA RTX 3080'
    ]
    amd_gpus = [
        'AMD Radeon RX 560', 'AMD Radeon RX 570', 'AMD Radeon RX 580',
        'AMD Radeon RX 5600 XT', 'AMD Radeon RX 5700 XT',
        'AMD Radeon RX 6600 XT', 'AMD Radeon RX 6700 XT', 'AMD Radeon RX 6800 XT'
    ]
    gpus = nvidia_gpus + amd_gpus
    vram = ['2GB', '3GB', '4GB', '6GB', '8GB', '10GB', '12GB', '16GB']
    
    return f"{np.random.choice(gpus)} {np.random.choice(vram)}"

# Function to convert memory string to GB
def convert_memory_to_gb(memory_str):
    if pd.isnull(memory_str):
        return None
    
    memory_str = str(memory_str).upper()
    match = re.search(r'(\d+)\s*(MB|GB)', memory_str)
    if not match:
        return None
    
    value = float(match.group(1))
    unit = match.group(2)
    
    if unit == 'MB':
        value = value / 1024
    
    return value

# Function to generate random memory requirement
def generate_memory_req():
    # Common memory sizes in GB
    memory_sizes = [2, 4, 6, 8, 12, 16, 24, 32, 64]
    weights = [0.05, 0.1, 0.15, 0.25, 0.2, 0.15, 0.05, 0.03, 0.02]  # Probability weights
    
    memory = np.random.choice(memory_sizes, p=weights)
    if memory < 4:  # For smaller values, sometimes use MB
        if random.random() < 0.3:  # 30% chance to use MB
            return f"{int(memory * 1024)}MB"
    return f"{memory}GB"

# Function to generate random storage requirement
def generate_storage_req():
    # Common storage sizes in GB
    storage_sizes = [
        5, 10, 15, 20, 30, 40, 50, 60, 80, 
        100, 120, 150, 200, 250, 300, 500
    ]
    weights = [
        0.05, 0.05, 0.05, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
        0.05, 0.05, 0.05, 0.03, 0.03, 0.02, 0.02
    ]
    
    storage = np.random.choice(storage_sizes, p=weights)
    return f"{storage}GB"

# Function to create feature matrix
def create_features(df):
    df['hardware_num'] = pd.factorize(df['hardware_id'])[0]
    X = df[['hardware_num']].copy()
    return X

# Fill null values in hardware_df
def fill_hardware_requirements(hardware_df, epic_games_df):
    print("\nStarting data filling process...")
    
    progress_bar = tqdm(total=5, desc="Filling Requirements", position=0)
    
    X = create_features(hardware_df)
    
    # Fill operating system
    if hardware_df['operacional_system'].isnull().any():
        os_options = ['Windows 10 64-bit', 'Windows 11 64-bit', 'Windows 10/11 64-bit']
        hardware_df['operacional_system'] = hardware_df['operacional_system'].apply(
            lambda x: np.random.choice(os_options) if pd.isnull(x) else x
        )
    progress_bar.update(1)
    progress_bar.set_description("OS Requirements Filled")
    
    # Fill processor requirements
    if hardware_df['processor'].isnull().any():
        null_count = hardware_df['processor'].isnull().sum()
        with tqdm(total=null_count, desc="Generating Processor Specs", position=1, leave=False) as pbar:
            hardware_df['processor'] = hardware_df['processor'].apply(
                lambda x: (generate_processor_req(), pbar.update(1))[0] if pd.isnull(x) else x
            )
    progress_bar.update(1)
    progress_bar.set_description("Processor Requirements Filled")
    
    # Fill memory requirements
    if hardware_df['memory'].isnull().any():
        null_count = hardware_df['memory'].isnull().sum()
        with tqdm(total=null_count, desc="Generating Memory Specs", position=1, leave=False) as pbar:
            hardware_df['memory'] = hardware_df['memory'].apply(
                lambda x: (generate_memory_req(), pbar.update(1))[0] if pd.isnull(x) else x
            )
    progress_bar.update(1)
    progress_bar.set_description("Memory Requirements Filled")
    
    # Fill graphics requirements
    if hardware_df['graphics'].isnull().any():
        null_count = hardware_df['graphics'].isnull().sum()
        with tqdm(total=null_count, desc="Generating Graphics Specs", position=1, leave=False) as pbar:
            hardware_df['graphics'] = hardware_df['graphics'].apply(
                lambda x: (generate_graphics_req(), pbar.update(1))[0] if pd.isnull(x) else x
            )
    progress_bar.update(1)
    progress_bar.set_description("Graphics Requirements Filled")
    
    # Fill storage requirements
    if hardware_df['storage'].isnull().any():
        null_count = hardware_df['storage'].isnull().sum()
        with tqdm(total=null_count, desc="Generating Storage Specs", position=1, leave=False) as pbar:
            hardware_df['storage'] = hardware_df['storage'].apply(
                lambda x: (generate_storage_req(), pbar.update(1))[0] if pd.isnull(x) else x
            )
    progress_bar.update(1)
    progress_bar.set_description("Storage Requirements Filled")
    
    # Clean up temporary columns
    hardware_df.drop('hardware_num', axis=1, inplace=True)
    
    progress_bar.close()
    return hardware_df

# Process the data
print("Starting data processing...")
filled_hardware_df = fill_hardware_requirements(hardware_df, epic_games_df)

# Save the processed data
print("\nSaving filled data to CSV...")
filled_hardware_df.to_csv('datasets/necessary_hardware_filled.csv', index=False)
print("Process completed successfully!")