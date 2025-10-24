# -- ----------------------------------
# -- Clean and transform fund transfer data
# -- Join fund transfers with account information to get customer IDs
# -- Links payer and payee accounts to cust

from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, avg, lit, year

@dp.materialized_view()
def fund_trans_silver():
    fund = spark.read.table("fund_trans_bronze")
    acct = spark.read.table("account_bronze")

    payer = acct.alias("payer_account")
    payee = acct.alias("payee_account")
    f = fund.alias("fund")

    return (
        f
        .join(payer, on=col("fund.payer_acc_id") == col("payer_account.id"), how="left")
        .join(payee, on=col("fund.payee_acc_id") == col("payee_account.id"), how="left")
        .select(
            col("payer_account.cust_id").alias("payer_cust_id"),
            col("payee_account.cust_id").alias("payee_cust_id"),
            col("fund.*")
        )
    )

# -- ----------------------------------
# -- Clean and transform customer data
# -- - Join customer with relationship data
# -- - Extract birth year from date of birth
# -- - Remove rescued data and unnecessary columns
# -- Creates comprehensive customer profile for downstream analysis
# -- ----------------------------------

@dp.materialized_view()
def customer_silver():
    # Read upstream DLT tables (batch reads => materialized view semantics)
    customer_df = spark.read.table("customer_bronze")
    relationship_df = spark.read.table("relationship_bronze")

    # Columns to keep (replicates SELECT ... EXCEPT(...))
    cust_keep = [c for c in customer_df.columns if c not in ("dob", "_rescued_data")]
    rel_keep = [c for c in relationship_df.columns if c not in ("_rescued_data", "id", "operation")]

    # Alias for disambiguation in joins/selections
    customer = customer_df.alias("customer")
    relationship = relationship_df.alias("relationship")

    # Join
    joined = customer.join(
        relationship,
        on=col("customer.id") == col("relationship.cust_id"),
        how="left"
    )

    # Select all desired columns + derived birth_year
    return joined.select(
        *[col(f"customer.{c}") for c in cust_keep],
        *[col(f"relationship.{c}") for c in rel_keep],
        year(col("customer.dob")).alias("birth_year")
    )

# -- ----------------------------------
# -- Clean and transform account data
# -- - Calculate account aggregations per customer (count, average balance)
# -- - Filter USD currency accounts
# -- Provides financial summary for credit decisioning
# -- ----------------------------------


@dp.materialized_view()
def account_silver():
    # Source table
    acc_bronze = spark.read.table("account_bronze")

    # CTE cust_acc: count accounts and average balance per customer
    cust_acc = (
        acc_bronze
        .groupBy("cust_id")
        .agg(
            count(lit(1)).alias("num_accs"),
            avg(col("balance")).alias("avg_balance")
        )
    )

    # USD slice of accounts
    acc_usd = (
        acc_bronze
        .filter(col("currency") == "USD")
        .select(
            "cust_id",
            col("balance").alias("balance_usd"),
            col("available_balance").alias("available_balance_usd"),
            "operation"
        )
    )

    # Final join
    return (
        cust_acc
        .join(acc_usd, on="cust_id", how="left")
        .select(
            col("cust_id"),
            col("num_accs"),
            col("avg_balance"),
            col("balance_usd"),
            col("available_balance_usd"),
            col("operation")
        )
    )