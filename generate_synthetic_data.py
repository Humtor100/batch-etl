import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/airflow"

engine = create_engine(DATABASE_URL)

def generate_transactions(num_transactions=1000):
    categories = ['Супермаркеты', 'Такси', 'Рестораны', 'Аптеки', 'Одежда', 'Переводы']
    transactions = []

    now = datetime.now()

    for i in range(1, num_transactions + 1):
        client_id = random.randint(1, 50)
        amount = round(random.uniform(50.0, 15000.0), 2)
        category = random.choice(categories)

        random_seconds = random.randint(0, 7 * 24 * 60 * 60)
        transaction_date = now - timedelta(seconds=random_seconds)

        transaction = {
            "transaction_id": i,
            "client_id": client_id,
            "amount": amount,
            "category": category,
            "transaction_date": transaction_date
        }
        transactions.append(transaction)

    return transactions

def save_to_postgres(transactions):
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE raw_transactions;"))
        connection.commit()

        insert_query = text("""
            INSERT INTO raw_transactions (transaction_id, client_id, amount, category, transaction_date)
            VALUES (:transaction_id, :client_id, :amount, :category, :transaction_date);
        """)

        connection.execute(insert_query, transactions)
        connection.commit()

        print(f"Успешно записано {len(transactions)} транзакций!")

if __name__ == "__main__":
    data = generate_transactions(1000)
    save_to_postgres(data)