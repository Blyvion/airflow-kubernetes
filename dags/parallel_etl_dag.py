from datetime import datetime
import time

from airflow.sdk import dag, task
from kubernetes.client import models as k8s


# ---------------------------------------------------------
# Kubernetes configuration for parallel transform tasks
# ---------------------------------------------------------

transform_executor_config = {
    "pod_override": k8s.V1Pod(

        # Give all transform Pods the same label so Kubernetes
        # knows they belong to the same group for spreading.
        metadata=k8s.V1ObjectMeta(
            labels={
                "parallel-transform": "true"
            }
        ),

        spec=k8s.V1PodSpec(

            # Airflow KubernetesExecutor expects the base
            # container when overriding the worker Pod.
            containers=[
                k8s.V1Container(
                    name="base",

                    resources=k8s.V1ResourceRequirements(
                        requests={
                            "cpu": "500m",
                            "memory": "256Mi",
                        },
                        limits={
                            "cpu": "1",
                            "memory": "512Mi",
                        },
                    ),
                )
            ],

            # Tell Kubernetes to spread matching Pods
            # across different worker nodes.
            topology_spread_constraints=[
                k8s.V1TopologySpreadConstraint(

                    # Difference between the most-loaded and
                    # least-loaded nodes cannot exceed 1.
                    max_skew=1,

                    # Spread across physical Kubernetes nodes.
                    topology_key="kubernetes.io/hostname",

                    # If Kubernetes cannot satisfy the rule,
                    # leave the Pod Pending rather than putting
                    # everything on one node.
                    when_unsatisfiable="DoNotSchedule",

                    # This constraint applies to Pods with
                    # this label.
                    label_selector=k8s.V1LabelSelector(
                        match_labels={
                            "parallel-transform": "true"
                        }
                    ),
                )
            ],
        ),
    )
}


# ---------------------------------------------------------
# DAG definition
# ---------------------------------------------------------

@dag(
    dag_id="kubernetes_parallel_etl_lab",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "kubernetes",
        "parallel",
        "etl",
        "resources",
        "topology-spread",
    ],
)
def kubernetes_parallel_etl_lab():

    # -----------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # TRANSFORM 1
    # -----------------------------------------------------

    @task(executor_config=transform_executor_config)
    def transform_revenue(records):

        print("Starting revenue transformation...")

        # Keep Pod running so we can observe scheduling.
        time.sleep(30)

        result = []

        for record in records:
            result.append(
                {
                    "product": record["product"],
                    "revenue": (
                        record["quantity"]
                        * record["price"]
                    ),
                }
            )

        print("Revenue transformation complete.")

        return result

    # -----------------------------------------------------
    # TRANSFORM 2
    # -----------------------------------------------------

    @task(executor_config=transform_executor_config)
    def transform_quantity(records):

        print("Starting quantity transformation...")

        time.sleep(30)

        total_quantity = sum(
            record["quantity"]
            for record in records
        )

        print(
            f"Total quantity sold: {total_quantity}"
        )

        return total_quantity

    # -----------------------------------------------------
    # TRANSFORM 3
    # -----------------------------------------------------

    @task(executor_config=transform_executor_config)
    def transform_average_price(records):

        print("Starting average price transformation...")

        time.sleep(30)

        average_price = (
            sum(
                record["price"]
                for record in records
            )
            / len(records)
        )

        print(
            f"Average product price: "
            f"${average_price:.2f}"
        )

        return average_price

    # -----------------------------------------------------
    # LOAD
    # -----------------------------------------------------

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

        print("--------------------------------")
        print(
            f"Total Revenue: ${total_revenue:.2f}"
        )
        print(
            f"Total Quantity: {total_quantity}"
        )
        print(
            f"Average Price: ${average_price:.2f}"
        )
        print("--------------------------------")

    # -----------------------------------------------------
    # DAG relationships
    # -----------------------------------------------------

    raw_data = extract()

    # These three become runnable at the same time.
    revenue = transform_revenue(raw_data)
    quantity = transform_quantity(raw_data)
    average_price = transform_average_price(
        raw_data
    )

    # load waits for all three parallel transformations.
    load(
        revenue,
        quantity,
        average_price,
    )


kubernetes_parallel_etl_lab()
