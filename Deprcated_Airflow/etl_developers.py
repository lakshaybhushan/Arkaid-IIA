import csv
from datetime import datetime

def extract_developers():
    """Extract developer data from CSV file"""
    developers = []
    with open('data_sources/developers.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert Active field to boolean
            row['Active'] = bool(int(row['Active']))
            
            # Parse date string to datetime
            row['Est.'] = datetime.strptime(row['Est.'], '%Y-%m-%d %H:%M:%S')
            
            developers.append(row)
    return developers

def transform_developers(developers):
    """Transform developer data"""
    transformed = []
    for dev in developers:
        transformed.append({
            'name': dev['Developer'],
            'is_active': dev['Active'],
            'city': dev['City'],
            'country': dev['Country'],
            'founded_date': dev['Est.'],
            'notable_games': dev['Notable games, series or franchises'],
            'notes': dev['Notes'] if dev['Notes'] else None
        })
    return transformed

def load_developers(developers):
    """Load developer data into database"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Drop and recreate developers table
    cur.execute("""
        DROP TABLE IF EXISTS developers;
        CREATE TABLE developers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            is_active BOOLEAN NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            founded_date TIMESTAMP NOT NULL,
            notable_games TEXT,
            notes TEXT
        );
    """)

    # Insert transformed data
    for dev in developers:
        cur.execute("""
            INSERT INTO developers (
                name, is_active, city, country, founded_date, notable_games, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            dev['name'],
            dev['is_active'], 
            dev['city'],
            dev['country'],
            dev['founded_date'],
            dev['notable_games'],
            dev['notes']
        ))

    conn.commit()
    cur.close()
    conn.close()

def etl_developers():
    """Execute ETL pipeline for developers data"""
    developers = extract_developers()
    transformed = transform_developers(developers)
    load_developers(transformed)

if __name__ == '__main__':
    etl_developers() 