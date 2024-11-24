import pandas as pd
from tqdm import tqdm

def load_city_country_mappings() -> dict:
    """Return a comprehensive dictionary of cities and their countries"""
    
    return {
        # United States Cities
        'Seattle': 'United States',
        'New York': 'United States',
        'Los Angeles': 'United States',
        'Chicago': 'United States',
        'Houston': 'United States',
        'Phoenix': 'United States',
        'Philadelphia': 'United States',
        'San Antonio': 'United States',
        'San Diego': 'United States',
        'Dallas': 'United States',
        'San Jose': 'United States',
        'Austin': 'United States',
        'Boston': 'United States',
        'Miami': 'United States',
        'San Francisco': 'United States',
        'Denver': 'United States',
        'Portland': 'United States',
        'Las Vegas': 'United States',
        'Atlanta': 'United States',
        'Orlando': 'United States',

        # Canadian Cities
        'Toronto': 'Canada',
        'Vancouver': 'Canada',
        'Montreal': 'Canada',
        'Calgary': 'Canada',
        'Edmonton': 'Canada',
        'Ottawa': 'Canada',
        'Quebec City': 'Canada',
        'Winnipeg': 'Canada',
        'Halifax': 'Canada',
        'Victoria': 'Canada',

        # European Cities
        'London': 'United Kingdom',
        'Manchester': 'United Kingdom',
        'Liverpool': 'United Kingdom',
        'Birmingham': 'United Kingdom',
        'Edinburgh': 'United Kingdom',
        'Glasgow': 'United Kingdom',
        
        'Paris': 'France',
        'Lyon': 'France',
        'Marseille': 'France',
        'Bordeaux': 'France',
        'Nice': 'France',
        'Toulouse': 'France',
        'Strasbourg': 'France',
        
        'Berlin': 'Germany',
        'Munich': 'Germany',
        'Hamburg': 'Germany',
        'Frankfurt': 'Germany',
        'Cologne': 'Germany',
        'Stuttgart': 'Germany',
        'Dresden': 'Germany',
        
        'Amsterdam': 'Netherlands',
        'Rotterdam': 'Netherlands',
        'The Hague': 'Netherlands',
        'Utrecht': 'Netherlands',
        
        'Stockholm': 'Sweden',
        'Gothenburg': 'Sweden',
        'Malmö': 'Sweden',
        
        'Oslo': 'Norway',
        'Bergen': 'Norway',
        
        'Copenhagen': 'Denmark',
        'Aarhus': 'Denmark',
        
        'Helsinki': 'Finland',
        'Tampere': 'Finland',
        
        'Dublin': 'Ireland',
        'Cork': 'Ireland',
        'Galway': 'Ireland',

        # Asian Cities
        'Tokyo': 'Japan',
        'Osaka': 'Japan',
        'Kyoto': 'Japan',
        'Yokohama': 'Japan',
        'Sapporo': 'Japan',
        'Nagoya': 'Japan',
        'Fukuoka': 'Japan',
        
        'Seoul': 'South Korea',
        'Busan': 'South Korea',
        'Incheon': 'South Korea',
        'Daegu': 'South Korea',
        
        'Beijing': 'China',
        'Shanghai': 'China',
        'Guangzhou': 'China',
        'Shenzhen': 'China',
        'Chengdu': 'China',
        'Hangzhou': 'China',
        'Wuhan': 'China',
        
        'Singapore': 'Singapore',
        
        'Mumbai': 'India',
        'Delhi': 'India',
        'Bangalore': 'India',
        'Hyderabad': 'India',
        'Chennai': 'India',
        'Kolkata': 'India',
        'Pune': 'India',

        # Australian Cities
        'Sydney': 'Australia',
        'Melbourne': 'Australia',
        'Brisbane': 'Australia',
        'Perth': 'Australia',
        'Adelaide': 'Australia',
        'Gold Coast': 'Australia',
        'Canberra': 'Australia',

        # South American Cities
        'São Paulo': 'Brazil',
        'Rio de Janeiro': 'Brazil',
        'Brasília': 'Brazil',
        'Salvador': 'Brazil',
        'Fortaleza': 'Brazil',
        
        'Buenos Aires': 'Argentina',
        'Córdoba': 'Argentina',
        'Rosario': 'Argentina',
        
        'Santiago': 'Chile',
        'Valparaíso': 'Chile',
        
        'Lima': 'Peru',
        'Cusco': 'Peru',
        
        'Bogotá': 'Colombia',
        'Medellín': 'Colombia',
        'Cali': 'Colombia',

        # Tech Hub Cities
        'Tel Aviv': 'Israel',
        'Jerusalem': 'Israel',
        'Haifa': 'Israel',
        
        'Dubai': 'United Arab Emirates',
        'Abu Dhabi': 'United Arab Emirates',
        
        'Moscow': 'Russia',
        'Saint Petersburg': 'Russia',
        'Novosibirsk': 'Russia',
        
        'Warsaw': 'Poland',
        'Kraków': 'Poland',
        'Wrocław': 'Poland',
        
        'Prague': 'Czech Republic',
        'Brno': 'Czech Republic',
        
        'Budapest': 'Hungary',
        'Debrecen': 'Hungary',
        
        'Bucharest': 'Romania',
        'Cluj-Napoca': 'Romania',
        
        'Vienna': 'Austria',
        'Salzburg': 'Austria',
        
        'Zürich': 'Switzerland',
        'Geneva': 'Switzerland',
        'Basel': 'Switzerland',
        
        'Brussels': 'Belgium',
        'Antwerp': 'Belgium',
        'Ghent': 'Belgium',
        
        'Madrid': 'Spain',
        'Barcelona': 'Spain',
        'Valencia': 'Spain',
        'Seville': 'Spain',
        
        'Lisbon': 'Portugal',
        'Porto': 'Portugal',
        
        'Rome': 'Italy',
        'Milan': 'Italy',
        'Florence': 'Italy',
        'Venice': 'Italy',
        'Naples': 'Italy',
        'Turin': 'Italy'
    }

def fix_city_country_mapping(input_file: str, output_file: str) -> None:
    """
    Fix the mismatch between City and Country in the developer dataset
    
    Args:
        input_file: Path to the input CSV file
        output_file: Path to save the corrected CSV file
    """
    
    # Load the comprehensive city-country mapping
    city_country_map = load_city_country_mappings()
    
    # Read the CSV file
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Update the dataframe
    print("Fixing city-country mismatches...")
    with tqdm(total=1) as pbar:
        # Update countries based on cities
        df['Country'] = df['City'].map(city_country_map).fillna(df['Country'])
        pbar.update(1)
    
    # Drop the Autonomous area column
    df = df.drop('Autonomous area', axis=1)
    
    # Save the corrected data
    print(f"Saving corrected data to {output_file}...")
    df.to_csv(output_file, index=False)
    
    # Print statistics
    print("\nLocation fixes completed!")
    print("Updated mappings:")
    for _, row in df.iterrows():
        print(f"City: {row['City']} -> {row['Country']}")
    
    # Print total number of cities covered
    total_cities = len(city_country_map)
    print(f"\nTotal number of cities covered: {total_cities}")

if __name__ == "__main__":
    INPUT_FILE = "datasets/developper.csv"
    OUTPUT_FILE = "datasets/developper_corrected.csv"
    
    fix_city_country_mapping(INPUT_FILE, OUTPUT_FILE)
