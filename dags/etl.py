from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

# 4. Створіть DAG і палаштуйте, щоб він запускався щогодинно.
# 5. Для завдання extract просто згенеруйте якісь дані у вигляді Json.
# 6. Для завдання transform потрібно зменшити вкладеність json.
# 7. Для завдання load створіть data frame і виведіть в консоль.

@dag(schedule='@hourly', start_date=datetime(2025, 5, 26), catchup=False)
def etl_pipeline():
    @task
    def extract():
        data = [
            {
                "user": {
                    "id": 1,
                    "name": "Ivan",
                    "contact": {
                        "email": "ivan@example.com",
                        "phone": "123456789"
                    }
                }
            },
            {
                "user": {
                    "id": 2,
                    "name": "Petro",
                    "contact": {
                        "email": "petro@example.com",
                        "phone": "987654321"
                    }
                }
            }
        ]
        return data

    @task
    def transform(data: list[dict]):
        print("Transforming data...", data)
        return [
            {
                "id": item["user"]["id"],
                "name": item["user"]["name"],
                "email": item["user"]["contact"]["email"],
                "phone": item["user"]["contact"]["phone"]
            } for item in data
        ]

    @task
    def load(data):
        print("Loading data...")
        print(data)
        df = pd.DataFrame(data)
        return data

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

etl_pipeline()  # Запуск DAG