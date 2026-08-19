from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="kubernetes_etl_lab",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kubernetes", "etl", "lab"],
)
def kubernetes_etl_lab():

    @task
    def extract():
        print("Extracting sales data...")

        return [
            {
                "product": "Laptop",
                "quantity": 2,
                "price": 900.00,
            },
            {
                "product": "Monitor",
                "quantity": 4,
                "price": 250.00,
            },
            {
                "product": "Keyboard",
                "quantity": 10,
                "price": 75.00,
            },
        ]

    @task
    def transform(records):
        print("Transforming records...")

        transformed = []

        for record in records:
            transformed.append(
                {
                    "product": record["product"],
                    "revenue":
                        record["quantity"] * record["price"],
                }
            )

        return transformed

    @task
    def load(records):
        print("Loading records...")

        total_revenue = 0

        for record in records:
            print(
                f'{record["product"]}: '
                f'${record["revenue"]:.2f}'
            )

            total_revenue += record["revenue"]

        print(f"Total Revenue: ${total_revenue:.2f}")

    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)


kubernetes_etl_lab()
