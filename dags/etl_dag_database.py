from datetime import datetime
import time

from airflow.sdk import dag, task, get_current_context

from airflow.providers.postgres.hooks.postgres import PostgresHook

from kubernetes.client import models as k8s


# ============================================================
# Kubernetes configuration for parallel transformation tasks
# ============================================================

transform_executor_config = {
    "pod_override": k8s.V1Pod(

        metadata=k8s.V1ObjectMeta(
            labels={
                "parallel-transform": "true"
            }
        ),

        spec=k8s.V1PodSpec(

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

            topology_spread_constraints=[
                k8s.V1TopologySpreadConstraint(

                    max_skew=1,

                    topology_key="kubernetes.io/hostname",

                    when_unsatisfiable="DoNotSchedule",

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


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="kubernetes_parallel_database_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,

    tags=[
        "kubernetes",
        "parallel",
        "etl",
        "postgres",
        "analytics",
    ],
)
def kubernetes_parallel_etl_lab():


    # ========================================================
    # EXTRACT
    # ========================================================

    @task
    def extract():

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


    # ========================================================
    # TRANSFORM: revenue
    # ========================================================

    @task(
        executor_config=transform_executor_config
    )
    def transform_revenue(records):

        time.sleep(30)

        result = []

        for record in records:

            result.append(
                {
                    "product": record["product"],

                    "revenue":
                        record["quantity"]
                        * record["price"],
                }
            )

        return result


    # ========================================================
    # TRANSFORM: quantity
    # ========================================================

    @task(
        executor_config=transform_executor_config
    )
    def transform_quantity(records):

        time.sleep(30)

        return sum(
            record["quantity"]
            for record in records
        )


    # ========================================================
    # TRANSFORM: average price
    # ========================================================

    @task(
        executor_config=transform_executor_config
    )
    def transform_average_price(records):

        time.sleep(30)

        return (
            sum(
                record["price"]
                for record in records
            )
            / len(records)
        )


    # ========================================================
    # LOAD
    # ========================================================

    @task
    def load(
        revenue_records,
        total_quantity,
        average_price,
    ):

        # Get the Airflow DAG run ID.
        context = get_current_context()

        run_id = context["ti"].run_id


        # ----------------------------------------------------
        # Calculate overall revenue
        # ----------------------------------------------------

        total_revenue = sum(
            record["revenue"]
            for record in revenue_records
        )


        # ----------------------------------------------------
        # Connect to the ANALYTICS PostgreSQL database.
        #
        # This does NOT connect to Airflow's metadata DB.
        # ----------------------------------------------------

        postgres = PostgresHook(
            postgres_conn_id="analytics_postgres"
        )


        # ----------------------------------------------------
        # Create analytics tables if they don't exist.
        # ----------------------------------------------------

        postgres.run(
            """
            CREATE TABLE IF NOT EXISTS sales_summary
            (
                run_id TEXT PRIMARY KEY,

                total_revenue NUMERIC(12, 2)
                    NOT NULL,

                total_quantity INTEGER
                    NOT NULL,

                average_price NUMERIC(12, 2)
                    NOT NULL,

                loaded_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW()
            );
            """,

            autocommit=True,
        )


        postgres.run(
            """
            CREATE TABLE IF NOT EXISTS product_revenue
            (
                run_id TEXT NOT NULL,

                product TEXT NOT NULL,

                revenue NUMERIC(12, 2)
                    NOT NULL,

                loaded_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW(),

                PRIMARY KEY
                (
                    run_id,
                    product
                )
            );
            """,

            autocommit=True,
        )


        # ----------------------------------------------------
        # Insert / update summary
        # ----------------------------------------------------

        postgres.run(
            """
            INSERT INTO sales_summary
            (
                run_id,
                total_revenue,
                total_quantity,
                average_price
            )

            VALUES
            (
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (run_id)

            DO UPDATE SET

                total_revenue =
                    EXCLUDED.total_revenue,

                total_quantity =
                    EXCLUDED.total_quantity,

                average_price =
                    EXCLUDED.average_price,

                loaded_at =
                    NOW();
            """,

            parameters=(
                run_id,
                total_revenue,
                total_quantity,
                average_price,
            ),

            autocommit=True,
        )


        # ----------------------------------------------------
        # Insert product-level analytics
        # ----------------------------------------------------

        connection = postgres.get_conn()

        try:

            with connection.cursor() as cursor:

                for record in revenue_records:

                    cursor.execute(
                        """
                        INSERT INTO product_revenue
                        (
                            run_id,
                            product,
                            revenue
                        )

                        VALUES
                        (
                            %s,
                            %s,
                            %s
                        )

                        ON CONFLICT
                        (
                            run_id,
                            product
                        )

                        DO UPDATE SET

                            revenue =
                                EXCLUDED.revenue,

                            loaded_at =
                                NOW();
                        """,

                        (
                            run_id,
                            record["product"],
                            record["revenue"],
                        ),
                    )

            connection.commit()

        finally:

            connection.close()


        return {
            "run_id": run_id,

            "total_revenue":
                total_revenue,

            "total_quantity":
                total_quantity,

            "average_price":
                average_price,
        }


    # ========================================================
    # DAG relationships
    # ========================================================

    raw_data = extract()


    revenue = transform_revenue(
        raw_data
    )

    quantity = transform_quantity(
        raw_data
    )

    average_price = transform_average_price(
        raw_data
    )


    load(
        revenue,
        quantity,
        average_price,
    )


kubernetes_parallel_etl_lab()
