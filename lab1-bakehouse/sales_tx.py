from pyspark import pipelines as dp


@dp.table(
    name="sales_tx",
    comment="Raw bakery transactions streamed from samples.bakehouse.sales_transactions",
    table_properties={"quality": "bronze"},
)
def sales_tx():
    # spark.readStream.table(...) inside @dp.table ⇒ streaming table
    return spark.readStream.table("samples.bakehouse.sales_transactions")
