import pandas as pd
from tqdm import tqdm
import random

def filter_dev_pub_data(steam_file='steam_games.csv', 
                       epic_file='epic_games.csv',
                       developers_file='developers.csv',
                       publishers_file='publishers.csv',
                       extra_entries=25):
    try:
        print("Reading datasets...")
        # Read all datasets
        steam_df = pd.read_csv(steam_file)
        epic_df = pd.read_csv(epic_file)
        developers_df = pd.read_csv(developers_file)
        publishers_df = pd.read_csv(publishers_file)
        
        # Get developers from Steam and Epic
        print("Processing developers...")
        steam_devs = set()
        for devs in steam_df['Developers'].dropna():
            steam_devs.update([d.strip() for d in str(devs).split(',')])
        
        epic_devs = set(epic_df['developer'].dropna().str.strip())
        
        # Get publishers from Steam and Epic
        print("Processing publishers...")
        steam_pubs = set()
        for pubs in steam_df['Publishers'].dropna():
            steam_pubs.update([p.strip() for p in str(pubs).split(',')])
            
        epic_pubs = set(epic_df['publisher'].dropna().str.strip())
        
        # Combine all developers and publishers
        all_developers = steam_devs.union(epic_devs)
        all_publishers = steam_pubs.union(epic_pubs)
        
        # Filter developers
        print("Filtering developers...")
        existing_devs = developers_df[developers_df['Developer'].isin(all_developers)]
        non_existing_devs = developers_df[~developers_df['Developer'].isin(all_developers)]
        
        # Keep some extra developers
        extra_devs = non_existing_devs.sample(n=min(extra_entries, len(non_existing_devs)), 
                                            random_state=42)
        
        # Combine and save developers
        final_devs = pd.concat([existing_devs, extra_devs])
        final_devs.to_csv('developers_filtered.csv', index=False)
        
        # Filter publishers
        print("Filtering publishers...")
        existing_pubs = publishers_df[publishers_df['Publisher'].isin(all_publishers)]
        non_existing_pubs = publishers_df[~publishers_df['Publisher'].isin(all_publishers)]
        
        # Keep some extra publishers
        extra_pubs = non_existing_pubs.sample(n=min(extra_entries, len(non_existing_pubs)), 
                                            random_state=42)
        
        # Combine and save publishers
        final_pubs = pd.concat([existing_pubs, extra_pubs])
        final_pubs.to_csv('publishers_filtered.csv', index=False)
        
        # Print statistics
        print("\nFiltering Statistics:")
        print(f"Developers:")
        print(f"- Original count: {len(developers_df)}")
        print(f"- Matching count: {len(existing_devs)}")
        print(f"- Extra entries kept: {len(extra_devs)}")
        print(f"- Final count: {len(final_devs)}")
        
        print(f"\nPublishers:")
        print(f"- Original count: {len(publishers_df)}")
        print(f"- Matching count: {len(existing_pubs)}")
        print(f"- Extra entries kept: {len(extra_pubs)}")
        print(f"- Final count: {len(final_pubs)}")
        
        # Create backup of original files
        developers_df.to_csv('developers_backup.csv', index=False)
        publishers_df.to_csv('publishers_backup.csv', index=False)
        
        return final_devs, final_pubs
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None, None

if __name__ == "__main__":
    # Filter developers and publishers
    dev_df, pub_df = filter_dev_pub_data()
    
    if dev_df is not None and pub_df is not None:
        print("\nFiltering completed successfully!")
        print("New files created:")
        print("- developers_filtered.csv")
        print("- publishers_filtered.csv")
        print("Backups created:")
        print("- developers_backup.csv")
        print("- publishers_backup.csv")