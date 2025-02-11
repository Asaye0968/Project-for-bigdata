import asyncio
import logging
import pandas as pd
import psycopg2
from telethon import TelegramClient
from aiogoogletrans import Translator  # Async Google Translator

# my api information
api_id = '24404426'  # my API ID
api_hash = '0eaac78da5bd5a7a029edd91aa548347'  # my API Hash
phone_number = '+251968406877'  # my phone number

# PostgreSQL Database Credentials
DB_CONFIG = {
    'dbname': 'telegram_data',
    'user': 'postgres',  # Default superuser
    'password': 'hellopostgress',
    'host': 'localhost',
    'port': '5432'
}

# List of Telegram channels to scrape
channels = [
    'Deliver_Addis',
    'FoodInEthiopia1',
    'ahatifoods943024546'
]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create the Telethon client
client = TelegramClient('session_name', api_id, api_hash)

# Translator for Amharic to English
translator = Translator()


def connect_db():
    """Establish a connection to PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


def create_table():
    """Ensure the telegram_messages table exists and is ready for use."""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telegram_messages (
                id SERIAL PRIMARY KEY,
                text TEXT,
                original_text TEXT,
                timestamp TIMESTAMP,
                channel TEXT
            );
        """)
        conn.commit()
        logging.info("Checked/Created 'telegram_messages' table.")
    except Exception as e:
        logging.error(f"Error creating database table: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def save_to_database(data):
    """Save scraped Telegram data to PostgreSQL."""
    try:
        conn = connect_db()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO telegram_messages (text, original_text, timestamp, channel)
        VALUES (%s, %s, %s, %s)
        """

        for row in data:
            cursor.execute(insert_query, (row['text'], row['original_text'], row['timestamp'], row['channel']))

        conn.commit()
        logging.info(f"✅ Inserted {len(data)} rows into the database.")

    except Exception as e:
        logging.error(f"❌ Error saving to database: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_combined_telegram_data():
    """Load combined Telegram data into PostgreSQL."""
    try:
        conn = connect_db()
        cursor = conn.cursor()

        df = pd.read_csv("C:/Users/hp/Desktop/python project/combined_telegram_data.csv")

        if df.empty:
            logging.warning("⚠️ combined_telegram_data.csv is empty! No data to insert.")
            return  # Stop execution if file is empty

        print("✅ Data to be inserted (first 5 rows):")
        print(df.head())  # Debugging - Show sample data

        insert_query = """
        INSERT INTO telegram_messages (text, original_text, timestamp, channel)
        VALUES (%s, %s, %s, %s)
        """

        inserted_count = 0  # Track successful insertions

        for _, row in df.iterrows():
            try:
                print(f"Inserting row: {row['text'][:50]}...")  # Print first 50 characters
                cursor.execute(insert_query, (row['text'], row['original_text'], row['timestamp'], row['channel']))
                inserted_count += 1
            except Exception as row_error:
                logging.error(f"⚠️ Error inserting row: {row_error}")
        
        conn.commit()
        logging.info(f"✅ Successfully inserted {inserted_count} rows from combined_telegram_data.csv into the database.")

    except Exception as e:
        logging.error(f"❌ Error inserting combined Telegram data: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def clean_telegram_data(df):
    """Clean the Telegram data."""
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['timestamp'] = df['timestamp'].ffill()
    df['channel'] = df['channel'].fillna('Unknown')
    df = df[df['text'].str.strip().astype(bool)]
    return df


async def translate_text(text, src='am', dest='en'):
    """Translate text asynchronously using aiogoogletrans."""
    try:
        translated = await translator.translate(text, src=src, dest=dest)
        return translated.text
    except Exception as e:
        logging.error(f"Error translating text: {e}")
        return text  # Return original text if translation fails


async def scrape_telegram_channel(channel_name):
    """Scrape messages from a Telegram channel."""
    try:
        logging.info(f"Scraping data from {channel_name}...")
        channel = await client.get_entity(channel_name)
        messages = await client.get_messages(channel, limit=100)

        data = []
        for message in messages:
            if message.text:
                original_text = message.text
                translated_text = await translate_text(original_text)  # ✅ Await translation
                
                data.append({
                    'text': translated_text,
                    'original_text': original_text,
                    'timestamp': message.date,
                    'channel': channel_name
                })
                await asyncio.sleep(2)  # Delay to avoid rate limiting

        df = pd.DataFrame(data)
        df = clean_telegram_data(df)

        df.to_csv(f'{channel_name}_data.csv', index=False)
        save_to_database(data)  # Save to database
        logging.info(f"✅ Data from {channel_name} saved to {channel_name}_data.csv and database.")

    except Exception as e:
        logging.error(f"❌ Error scraping {channel_name}: {e}")


def combine_data(channels):
    """Combine data from all channels into a single DataFrame."""
    combined_df = pd.DataFrame()
    for channel_name in channels:
        try:
            df = pd.read_csv(f'{channel_name}_data.csv')
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        except FileNotFoundError:
            logging.warning(f"⚠️ No data found for {channel_name}. Skipping...")

    combined_df.to_csv('C:/Users/hp/Desktop/python project/combined_telegram_data.csv', index=False)
    logging.info("✅ Combined data saved to combined_telegram_data.csv")
    return combined_df


async def main():
    """Main function to scrape data and store it."""
    await client.start(phone_number)

    create_table()  # Ensure the database table exists

    for channel_name in channels:
        await scrape_telegram_channel(channel_name)

    combine_data(channels)  # Combine all data into one file

    load_combined_telegram_data()  # Load the combined dataset into the database


if __name__ == '__main__':
    with client:
        client.loop.run_until_complete(main()) 
