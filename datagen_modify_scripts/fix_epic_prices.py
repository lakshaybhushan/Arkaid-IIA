import pandas as pd

# Read the CSV file
df = pd.read_csv('datasets/epic-games.csv')

# Convert price column to float and divide by 100 to add decimal point
df['price'] = df['price'].apply(lambda x: '{:.2f}'.format(x/100) if pd.notnull(x) else x)

# Save the updated CSV file
df.to_csv('datasets/epic-games.csv', index=False)

print("Prices have been updated successfully!") 