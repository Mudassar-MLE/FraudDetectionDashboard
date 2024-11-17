import psycopg2
import random
from datetime import datetime
import time
import os
from faker import Faker
import pandas as pd

def run_rules(df):
    df['rules_triggered'] = None
    df['rules_explanation'] = None
    df['decision'] = None

    if df['amount'][0] >= 100 and df['account_blacklisted'][0] == False and df['trans_type'][0] == 'Real_time_transaction':
        df['rules_triggered'] = 'Rule1'
        df['rules_explanation'] = 'User is trying to make a transaction of more than 100$'
        df['decision'] = 'Rejected'
    elif df['account_blacklisted'][0] == True and df['trans_type'][0] == 'Real_time_transaction':
        df['rules_triggered'] = 'Rule2'
        df['rules_explanation'] = 'It is a blacklisted Card'
        df['decision'] = 'Rejected'
    elif df['trans_type'][0] != 'Real_time_transaction':
        df['rules_triggered'] = 'No Rules Triggered'
        df['decision'] = 'Approved'
    else:
        df['rules_triggered'] = 'No Rules Triggered'

    dict_index = df.to_dict(orient='index')
    dict_single_row = dict_index[list(dict_index.keys())[0]]
    return dict_single_row

def generate_record():
    card_type = random.choice(list(card_types.keys()))
    return {
        "uniq_id": [fake.uuid4()],
        "trans_type": [random.choice(["Real_time_transaction", "settlements", "dispute"])],
        "amount": [round(random.uniform(10.0, 1000.0), 2)],
        "amount_crr": [round(random.uniform(10.0, 1000.0), 2)],
        "account_holder_name": [fake.name()],
        "card_presense": [random.choice(["Present", "Not Present"])],
        "merchant_category": [random.choice(merchant_categories)],
        "card_type": [card_type],
        "card_id": [fake.credit_card_number(card_type=card_types[card_type])],
        "account_id": [fake.uuid4()],
        "account_blacklisted": [random.choice([True, False])]
    }

if __name__ == "__main__":
    # Initialize the Faker library
    fake = Faker()

    # Define the number of records you want to generate
    num_records = 10

    # List of merchant categories for random selection
    merchant_categories = [
        "Retail", "Electronics", "Clothing", "Groceries", "Pharmacy", 
        "Entertainment", "Dining", "Travel", "Utilities", "Healthcare"
    ]

    # List of card types for random selection
    card_types = {
        "visa": "visa",
        "mastercard": "mastercard"
    }

    # PostgreSQL connection details
    host = ""
    port = 
    dbname = ""
    user = ""
    password = ""

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

    cur = conn.cursor()

    # Create the banking_data table if it does not exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS banking_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL,
        uniq_id UUID NOT NULL,
        trans_type VARCHAR(50) NOT NULL,
        amount DECIMAL(10, 2) NOT NULL,
        amount_crr DECIMAL(10, 2) NOT NULL,
        account_holder_name VARCHAR(100) NOT NULL,
        card_presense VARCHAR(50) NOT NULL,
        merchant_category VARCHAR(50) NOT NULL,
        card_type VARCHAR(50) NOT NULL,
        card_id VARCHAR(20) NOT NULL,
        account_id UUID NOT NULL,
        account_blacklisted BOOLEAN NOT NULL,
        rules_triggered VARCHAR(100),
        rules_explanation VARCHAR(100),
        decision VARCHAR(100)
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    try:
        while True:
            timestamp = datetime.utcnow()
            for _ in range(num_records):
                temp = generate_record()
                df = pd.DataFrame(temp)
                record = run_rules(df)
                cur.execute("""
                INSERT INTO banking_data (timestamp, uniq_id, trans_type, amount, amount_crr, account_holder_name, card_presense, merchant_category, card_type, card_id, account_id, account_blacklisted, rules_triggered, rules_explanation, decision)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (timestamp, record['uniq_id'], record['trans_type'], record['amount'], record['amount_crr'],
                             record['account_holder_name'], record['card_presense'], record['merchant_category'],
                             record['card_type'], record['card_id'], record['account_id'], record['account_blacklisted'],
                             record['rules_triggered'], record['rules_explanation'], record['decision']))
                conn.commit()
            time.sleep(15)
    finally:
        cur.close()
        conn.close()