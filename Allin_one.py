import pandas as pd
import psycopg2
import logging
from psycopg2 import extras

# PostgreSQL Database Credentials
DB_CONFIG = {
    'dbname': 'yelp_database',
    'user': 'postgres',
    'password': 'hellopostgress',  #  actual password
    'host': 'localhost',
    'port': '5432'
}

# Load the cleaned dataset
file_path = "final_cleaned.csv"
df = pd.read_csv(file_path, dtype={'Phone': str})

# Convert Time_GMT to datetime format, handling errors
df['Time_GMT'] = pd.to_datetime(df['Time_GMT'], errors='coerce')

# Replace empty strings with None
df.replace('', None, inplace=True)

# Drop rows with missing critical data AFTER type conversion
df.dropna(subset=['Time_GMT', 'Phone', 'Organization'], inplace=True)

# Ensure numeric columns are valid, handling errors
df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
df['NumberReview'] = pd.to_numeric(df['NumberReview'], errors='coerce')



def connect_db():
    """Establish a connection to PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


def insert_data():
    conn = None
    cursor = None
    try:
        conn = connect_db()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO yelp_reviews (time_gmt, phone, organization, rating, number_review, 
                                  category, country, country_code, state, city, street, building)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        required_columns = ['Time_GMT', 'Phone', 'Organization', 'Rating', 'NumberReview', 
                            'Category', 'Country', 'CountryCode', 'State', 'City', 'Street', 'Building']

        try:
            df_to_insert = df[required_columns]
        except KeyError as e:
            logging.error(f"❌ Column missing in DataFrame: {e}. DataFrame columns are: {df.columns.tolist()}")
            return

        # ***CRITICAL: Clean up whitespace from string columns using .loc***
        for col in ['Organization', 'Category', 'Country', 'CountryCode', 'State', 'City', 'Street', 'Building', 'Phone']:
            if df_to_insert[col].dtype == 'object':
                df_to_insert.loc[:, col] = df_to_insert[col].str.strip()

        # ***CRITICAL: Handle NaT values in Time_GMT using .loc***
        df_to_insert.loc[:, 'Time_GMT'] = df_to_insert['Time_GMT'].fillna(pd.NaT).apply(lambda x: None if pd.isna(x) else x)

        data_tuples = [tuple(row) for row in df_to_insert.to_numpy()]

        # Debugging prints:
        num_tuples_to_print = min(5, len(data_tuples)) if len(data_tuples) > 0 else 0  # Check for empty data_tuples
        for i in range(num_tuples_to_print):
            print(f"Tuple {i+1}: {data_tuples[i]}, Length: {len(data_tuples[i])}")

        expected_values_per_tuple = insert_query.count('%s')
        print(f"Expected values per tuple: {expected_values_per_tuple}")

        # ***KEY CHANGE: Batch size for execute_batch***
        batch_size = 10000  # Adjust as needed

        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            try:
                extras.execute_batch(cursor, insert_query, batch)
                print(f"Inserted batch {i//batch_size + 1} of {len(data_tuples)//batch_size + (1 if len(data_tuples)%batch_size != 0 else 0)}")
                conn.commit()  # Moved commit inside the loop
            except Exception as e:
                logging.error(f"❌ Error inserting batch {i//batch_size + 1}: {e}")
                conn.rollback()  # Rollback only the failed batch
                break  # Stop inserting further batches if one fails

        # Check if all batches were inserted
        if i + batch_size >= len(data_tuples):
            logging.info(f"✅ Successfully inserted {len(df_to_insert)} rows into the database.")
        else:
            logging.info(f"⚠️ Insertion stopped at batch {i//batch_size + 1}. Check the logs for errors.")

    except Exception as e:
        logging.error(f"❌ General error during insertion: {e}")  # More general error handling
        if conn:
            conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Run the insertion
insert_data()
