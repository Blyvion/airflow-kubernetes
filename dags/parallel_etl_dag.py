from datetime import datetime
import time

from airflow.sdk import dag, task


@dag(
    dag_id="kubernetes_parallel_etl_lab",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kubernetes", "parallel", "etl", "lab"],
)
def kubernetes_parallel_etl_lab():

    @task
    def extract():
        print("Extracting sales data...")

        return [
            {"product": "Laptop", "quantity": 2, "price": 900.00},
            {"product": "Monitor", "quantity": 4, "price": 250.00},
            {"product": "Keyboard", "quantity": 10, "price": 75.00},
        ]

    @task
    def transform_revenue(records):
        print("Starting revenue transformation...")
        time.sleep(30)

        result = []

        for record in records:
            result.append({
                "product": record["product"],
                "revenue": record["quantity"] * record["price"],
            })

        print("Revenue transformation complete.")
        return result

    @task
    def transform_quantity(records):
        print("Starting quantity transformation...")
        time.sleep(30)

        total_quantity = sum(
            record["quantity"] for record in records
        )

        print(f"Total quantity sold: {total_quantity}")
        return total_quantity

    @task
    def transform_average_price(records):
        print("Starting average price transformation...")
        time.sleep(30)

        average_price = (
            sum(record["price"] for record in records)
            / len(records)
        )

        print(f"Average product price: ${average_price:.2f}")
        return average_price

    @task
    def load(
        revenue_records,
        total_quantity,
        average_price,
    ):
        print("Loading transformed results...")

        total_revenue = sum(
            record["revenue"]
            for record in revenue_records
        )

        print("-------------------------------")
        print(f"Total Revenue: ${total_revenue:.2f}")
        print(f"Total Quantity: {total_quantity}")
        print(f"Average Price: ${average_price:.2f}")
        print("-------------------------------")

    raw_data = extract()

    revenue = transform_revenue(raw_data)
    quantity = transform_quantity(raw_data)
    average_price = transform_average_price(raw_data)

    load(
        revenue,
        quantity,
        average_price,
    )


kubernetes_parallel_etl_lab()
