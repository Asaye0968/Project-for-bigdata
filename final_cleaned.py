
import pandas as pd

# Load the dataset
df = pd.read_csv("Clean_dataset.csv")
# Drop the 'OLF' column
df = df.drop(columns=['OLF'])

# Save the cleaned dataset
df.to_csv("final_cleaned.csv", index=False)

# Check if 'OLF' exists in the columns
if 'OLF' in df.columns:
    olf_usage_check = df['OLF'].isnull().sum()
    print(f"Number of null values in 'OLF' column: {olf_usage_check}")
else:
    print("Column 'OLF' does not exist in the dataset.")

