import pandas as pd
import numpy as np

def generate_biased_hours():
    # Generate random numbers with a beta distribution biased towards middle range
    # Beta distribution parameters chosen to create desired shape
    raw = np.random.beta(2, 5) 
    # Scale to our desired range (100-100000) and ensure float
    scaled = float(100 + (raw * 99900))  # Changed to 99900 to reach 100000
    # Return with 2 decimal places
    return round(scaled, 2)

def update_steam_games():
    # Read the CSV file
    df = pd.read_csv('new/steam_games.csv')
    
    # Generate average hours played for each row as float values
    avg_hours = pd.Series([generate_biased_hours() for _ in range(len(df))], dtype=float)
    
    # Update the existing column
    df['Average hours played'] = avg_hours
    
    # Save the updated dataframe back to CSV
    df.to_csv('new/steam_games.csv', index=False, float_format='%.2f')
    
    print("Steam games CSV has been updated successfully!")

if __name__ == "__main__":
    update_steam_games() 