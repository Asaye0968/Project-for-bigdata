import pandas as pd
# Define column data types
dtype_dict = {
    "Phone": str,  # Force phone numbers to be read as strings
    "Building": str,  # Ensure numeric-like values remain as strings if needed
    4: str,  # Ensure column 4 is read as string
    2: str   # Ensure column 2 is read as string
}

# Load dataset

# Load dataset
file_path = "yelp_database.csv"  # Update with your actual file path
df = pd.read_csv(file_path, dtype=dtype_dict)

dtype_dict = {4: str}

df = pd.read_csv(file_path, dtype=dtype_dict)
df = pd.read_csv(file_path, dtype=dtype_dict, low_memory=False)

# 1. Handle Missing Values
# Fill missing Rating with the average rating, or you can choose to drop them
df['Rating'] = df['Rating'].fillna(df['Rating'].mean())

# Fill missing NumberReview with 0, assuming no reviews means zero
df['NumberReview'] = df['NumberReview'].fillna(0)

# Fill missing Phone, Organization, and other critical fields with a placeholder (e.g., "Unknown")
df['Phone'] = df['Phone'].fillna("Unknown")
df['Organization'] = df['Organization'].fillna("Unknown")
df['Category'] = df['Category'].fillna("Unknown")
df['Country'] = df['Country'].fillna("Unknown")
df['State'] = df['State'].fillna("Unknown")
df['City'] = df['City'].fillna("Unknown")
df['Street'] = df['Street'].fillna("Unknown")
df['Building'] = df['Building'].fillna("Unknown")

# 2. Remove Duplicates
# Remove exact duplicates (where all columns are the same)
df = df.drop_duplicates()
#remove duplicated unusable column
# Drop the 'OLF' column


# 3. Format Data Types
# Convert Time_GMT to datetime format (handling errors)
df['Time_GMT'] = pd.to_datetime(df['Time_GMT'], errors='coerce')

df = df.drop(columns=['OLF'])

# Convert Rating and NumberReview to appropriate types (float and int)
df['Rating'] = df['Rating'].astype(float)
df['NumberReview'] = df['NumberReview'].astype(int)


# CHECK OLF COLUMN NOT NECESSARY IN MY CASE
#Check if 'OLF' exists in the columns
if 'OLF' in df.columns:
    olf_usage_check = df['OLF'].isnull().sum()
    print(f"Number of null values in 'OLF' column: {olf_usage_check}")
else:
    print("Column 'OLF' does not exist in the dataset.")


# 4. Handle Inconsistencies and Errors
# Remove leading/trailing whitespace from string columns
df['Organization'] = df['Organization'].str.strip()
df['Street'] = df['Street'].str.strip()
df['City'] = df['City'].str.strip()
df['State'] = df['State'].str.strip()
df['Country'] = df['Country'].str.strip()
df['Category'] = df['Category'].str.strip()
df['Building'] = df['Building'].str.strip()

# Handle inconsistent phone numbers (e.g., remove non-numeric characters)
df['Phone'] = df['Phone'].str.replace(r'\D', '', regex=True)  # Remove non-numeric characters

# Handle inconsistent CountryCode (e.g., ensure it's uppercase)
df['CountryCode'] = df['CountryCode'].str.upper()

# 5. Save Cleaned Dataset
cleaned_file_path = "Clean_dataset.csv"
df.to_csv(cleaned_file_path, index=False)

print(f"✅ Data cleaning completed. Cleaned dataset saved to {cleaned_file_path}.")
