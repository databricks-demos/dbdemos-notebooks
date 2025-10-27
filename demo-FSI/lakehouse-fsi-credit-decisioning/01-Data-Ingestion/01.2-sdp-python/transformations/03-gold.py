# -- ----------------------------------
# -- Clean credit bureau data with data quality constraints
# -- Validate that customer ID is not null
# -- Drop rows with missing customer IDs to ensure data quality
# -- ----------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view()
@dp.expect_or_drop("CustomerID_not_null", "CUST_ID IS NOT NULL")
def credit_bureau_gold():
    # Batch read from the LIVE streaming table => materialized output
    return spark.read.table("credit_bureau_bronze")
  

# -- ----------------------------------
# -- Create fund transfer aggregation features
# -- Calculate transaction metrics over 3, 6, and 12 month windows:
# -- - Count of distinct payers/payees
# -- - Count of sent/received transactions
# -- - Total and average transaction amounts
# -- Provides behavioral payment patterns for credit risk assessment
# -- ----------------------------------

@dp.materialized_view()
def fund_trans_gold():
    # Sources
    funds = spark.read.table("fund_trans_silver")
    customers = spark.read.table("customer_silver")

    # Compute scalar max date via cross-join (avoid collect)
    max_date_df = funds.select(F.to_date(F.max("datetime")).alias("max_dt"))

    # Helper to build aggregates for a period
    def agg_payer(months: int):
        return (
            funds.crossJoin(max_date_df)
                 .where(F.to_date(F.col("datetime")) >= F.add_months(F.col("max_dt"), -months))
                 .groupBy("payer_cust_id")
                 .agg(
                     F.countDistinct("payer_cust_id").alias(f"dist_payer_cnt_{months}m"),
                     F.count(F.lit(1)).alias(f"sent_txn_cnt_{months}m"),
                     F.sum("txn_amt").alias(f"sent_txn_amt_{months}m"),
                     F.avg("txn_amt").alias(f"sent_amt_avg_{months}m"),
                 )
        )

    def agg_payee(months: int):
        return (
            funds.crossJoin(max_date_df)
                 .where(F.to_date(F.col("datetime")) >= F.add_months(F.col("max_dt"), -months))
                 .groupBy("payee_cust_id")
                 .agg(
                     F.countDistinct("payee_cust_id").alias(f"dist_payee_cnt_{months}m"),
                     F.count(F.lit(1)).alias(f"rcvd_txn_cnt_{months}m"),
                     F.sum("txn_amt").alias(f"rcvd_txn_amt_{months}m"),
                     F.avg("txn_amt").alias(f"rcvd_amt_avg_{months}m"),
                 )
        )

    # Build all period aggregates
    p12 = agg_payer(12)
    r12 = agg_payee(12)
    p6  = agg_payer(6)
    r6  = agg_payee(6)
    p3  = agg_payer(3)
    r3  = agg_payee(3)

    # Join to customers and select excluding the grouping keys
    df = (
        customers
        .join(p12, customers.cust_id == p12.payer_cust_id, "left")
        .join(r12, customers.cust_id == r12.payee_cust_id, "left")
        .join(p6,  customers.cust_id == p6.payer_cust_id,  "left")
        .join(r6,  customers.cust_id == r6.payee_cust_id,  "left")
        .join(p3,  customers.cust_id == p3.payer_cust_id,  "left")
        .join(r3,  customers.cust_id == r3.payee_cust_id,  "left")
    )

    # Columns to drop (keys from the aggregated tables)
    drop_cols = [
        "payer_cust_id", "payee_cust_id"
    ]

    # Select: cust_id + all other columns except the grouping keys
    keep_cols = ["cust_id"] + [c for c in df.columns if c not in drop_cols and c != "cust_id"]

    return df.select(*[F.col(c) for c in keep_cols])
  


#   -- ----------------------------------
# -- Enrich telco partner data with customer information
# -- Join telco data with customer records using phone number
# -- Alternative data source to evaluate creditworthiness
# -- ----------------------------------

@dp.materialized_view()
def telco_gold():
    # Read upstream DLT tables (batch reads => materialized output)
    telco_df = spark.read.table("telco_bronze")
    customer_df = spark.read.table("customer_bronze")

    telco = telco_df.alias("telco")
    customer = customer_df.alias("customer")

    return (
        telco.join(customer, F.col("telco.user_phone") == F.col("customer.mobile_phone"), "left")
             .select(
                 F.col("customer.id").alias("cust_id"),
                 *[F.col(f"telco.{c}") for c in telco_df.columns]
             )
    )

# -- ----------------------------------
# -- Create comprehensive customer profile
# -- Join customer data with account aggregations
# -- System of record for customer attributes and financial summary
# -- ----------------------------------

@dp.materialized_view()
def customer_gold():
    customer = spark.read.table("customer_silver").alias("customer")
    account  = spark.read.table("account_silver").alias("account")

    return (
        customer.join(account, F.col("customer.cust_id") == F.col("account.cust_id"), "left")
                .select(
                    *[F.col(f"customer.{c}") for c in customer.columns],
                    F.col("account.avg_balance"),
                    F.col("account.num_accs"),
                    F.col("account.balance_usd"),
                    F.col("account.available_balance_usd")
                )
    )


# -- ----------------------------------
# -- Create secured customer view with PII masking
# -- Mask first name using AES encryption for data science users
# -- Best practice for protecting sensitive data with dynamic views
# -- Uses is_member function to encrypt based on user group
# -- ----------------------------------


@dp.materialized_view
def customer_gold_secured():
    # Read source
    c = spark.read.table("customer_gold").alias("c")

    # Get encryption key from secret scope
    secret_key = dbutils.secrets.get("<YOUR_SCOPE>", "<YOUR_SECRET_NAME>")

    # Mask logic: conditionally encrypt first_name for data-science-users
    masked_first_name = F.when(
        F.expr("is_member('data-science-users')"),
        F.expr(f"base64(aes_encrypt(c.first_name, '{secret_key}'))")
    ).otherwise(F.col("c.first_name")).alias("first_name")

    # Select all columns except first_name, and append the masked version
    keep_cols = [F.col(f"c.{col}") for col in c.columns if col != "first_name"]

    return c.select(*keep_cols, masked_first_name)
