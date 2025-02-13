![image](https://github.com/user-attachments/assets/3697e331-240f-4d69-b1bb-e58838553c29)
🚀 BIgdata-Project: End-to-End Data Pipeline

This repository contains "Python scripts" for cleaning, processing, and storing data from "Yelp" and "Telegram" sources. The project includes data scraping, cleaning, transformation, and storage in a "PostgreSQL database". It is designed to handle large datasets efficiently and ensure data integrity.
📂 Repository Structure
#Note: Other part of code are exists in my google drive link in below..
GOOGLE DRIVE LINK: "https://drive.google.com/drive/folders/15YQt1y_6SKD9oKeKHwlKCgE5f10Ox1LH?usp=sharing"

BIgdata-Project/
├── EthioScraber.py             # Telegram scraper and data pipeline
├── final_cleaned.py            # Yelp dataset cleaning script
├── test.py                     # Yelp data insertion into PostgreSQL
├── README.md                   # Project documentation
├── Transformingdata.py           # Python transformation code
├── Deliver_Addis_data.csv      # Scraped data from Telegram
├── FoodInEthiopia1_data.csv    # Scraped data from Telegram
├── ahatifoods943024546_data.csv# Scraped data from Telegram
├── combined_telegram_data.csv  # Combined Telegram data
├── final_cleaned.csv           # Cleaned Yelp dataset
└── yelp_database.csv           # Raw Yelp dataset
```
 Overview

 #Yelp Dataset Cleaning

This script cleans and preprocesses the "Yelp dataset" (`yelp_database.csv`). It ensures data consistency, removes duplicates, handles missing values, and standardizes formats for better usability.

#Features
- Reads and processes the Yelp dataset.
- Defines appropriate data types to prevent data loss.
- Handles missing values for critical fields like `Rating`, `Phone`, and `Organization`.
- Removes duplicate entries and unnecessary columns.
- Converts timestamps to `datetime` format and ensures correct numeric types.
- Standardizes phone numbers, country codes, and other categorical fields.
- Saves the cleaned dataset as `Clean_dataset.csv`.

#Installation
1. Install dependencies: "pip install pandas"
2. Run the script: "python final_cleaned.py"
3. Ensure `yelp_database.csv` is in the same directory or update the `file_path` variable in the script.

#Output
- Cleaned dataset saved as: "Clean_dataset.csv".

#2 Telegram Scraper & Data Pipeline
This script scrapes messages from **Telegram channels**, translates them (if needed), cleans the data, and stores it in a **PostgreSQL database**. The extracted data is also saved in CSV format for further analysis.

#Features
- Scrapes messages from Telegram channels using "Telethon".
- Translates Amharic messages to English using "aiogoogletrans".
- Cleans and formats data (handles missing values, timestamps, duplicates).
- Stores cleaned data in a "PostgreSQL database".
- Combines and saves extracted data into CSV files.

#Installation
1. Install dependencies: "pip install telethon pandas psycopg2 aiogoogletrans asyncio"

2. Set up PostgreSQL database:
   - Ensure PostgreSQL is installed and running.
   - Update the database credentials in `DB_CONFIG` within the script.
   - Create the `telegram_messages` table:
     ```sql
     CREATE TABLE telegram_messages (
         id SERIAL PRIMARY KEY,
         time_gmt TIMESTAMP,
         phone VARCHAR(20),
         organization VARCHAR(255),
         rating FLOAT,
         number_review INT,
         category VARCHAR(255),
         country VARCHAR(255),
         country_code VARCHAR(10),
         state VARCHAR(255),
         city VARCHAR(255),
         street VARCHAR(255),
         building VARCHAR(255)
     );
     ```
3. Run the script: " python EthioScraber.py"


#Configuration
- Modify the `channels` list in the script to add/remove Telegram channels:
   python
  channels = [
      'Deliver_Addis',
      'FoodInEthiopia1',
      'ahatifoods943024546'
  ]
  

#Output
- Scraped data is saved as individual CSV files per channel:
  ---
  Deliver_Addis_data.csv
  FoodInEthiopia1_data.csv
  ahatifoods943024546_data.csv
  ----
- All channel data is combined into: "combined_telegram_data.csv".
- Data is also inserted into the PostgreSQL database under the `telegram_messages` table.

---

#3 Yelp Reviews Data Processing & PostgreSQL Storage
This script processes "Yelp business reviews data" by cleaning, formatting, and storing it in a "PostgreSQL database". It ensures data integrity by handling missing values, converting data types, and inserting records efficiently in batches.

#Features
- Cleans raw Yelp data (removes empty/invalid fields).
- Converts time, numeric, and text data types correctly.
- Inserts cleaned data into a PostgreSQL `yelp_reviews` table.
- Uses "batch insertion" for better database performance.
- Logs errors and handles failed batch inserts gracefully.

#Installation
1. Install dependencies: " pip install pandas psycopg2"
   
2. Run the script to clean and save the dataset:  "python clean_dataset.py"
 
3. Load data into PostgreSQL: " python test.py"


#Configuration
- Modify the `file_path` variable in the script to point to your dataset:

  file_path = "final_cleaned.csv"
  

#Output
- Cleaned dataset saved as: `final_cleaned.csv`.
- Successfully inserted data is stored in the `yelp_reviews` table in PostgreSQL.



# Contributing
Feel free to fork this repository and submit pull requests to improve the data cleaning or scraping processes!

#License
This project is open-source and available under the "MIT License"

# Contact
For questions or feedback, please open an issue on GitHub or contact me on telegram: https://t.me/Bezawetalemn. 😊

