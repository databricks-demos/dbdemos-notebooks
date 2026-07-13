# Databricks notebook source
# MAGIC %pip install iso3166 Faker

# COMMAND ----------

# MAGIC %md
# MAGIC # Data generator for Lakeflow pipeline
# MAGIC This notebook will generate data in the given storage path to simulate a data flow. 
# MAGIC
# MAGIC **Make sure the storage path matches what you defined in your Lakeflow pipeline as input.**
# MAGIC
# MAGIC 1. Run Cmd 2 to show widgets
# MAGIC 2. Specify Storage path in widget
# MAGIC 3. "Run All" to generate your data
# MAGIC 4. When finished generating data, "Stop Execution"
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=00-Loan-Data-Generator&demo_name=dlt-loans&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Run First for Widgets
catalog = "main__build"
schema = dbName = db = "dbdemos_dlt_loan"

volume_name = "raw_data"

dbutils.widgets.combobox('reset_all_data', 'false', ['true', 'false'], 'Reset all existing data')
dbutils.widgets.combobox('batch_wait', '30', ['15', '30', '45', '60'], 'Speed (secs between writes)')
dbutils.widgets.combobox('num_recs', '10000', ['5000', '10000', '20000'], 'Volume (# records per writes)')
dbutils.widgets.combobox('batch_count', '1', ['1', '100', '200', '500'], 'Write count (how many times do we append data)')

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS `{catalog}`')
spark.sql(f'USE CATALOG `{catalog}`')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
spark.sql(f'USE SCHEMA `{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume_name}`')
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

import pyspark.sql.functions as F
output_path = volume_folder
dbutils.fs.mkdirs(output_path)
reset_all_data = dbutils.widgets.get('reset_all_data') == "true"


def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# COMMAND ----------

spark.read.csv('/databricks-datasets/lending-club-loan-stats', header=True) \
      .withColumn('id', F.monotonically_increasing_id()) \
      .withColumn('member_id', (F.rand()*1000000).cast('int')) \
      .withColumn('accounting_treatment_id', (F.rand()*6).cast('int')) \
      .repartition(50).write.mode('overwrite').option('header', True).format('csv').save(output_path+'/historical_loans')

spark.createDataFrame([
  (0, 'held_to_maturity'),
  (1, 'available_for_sale'),
  (2, 'amortised_cost'),
  (3, 'loans_and_recs'),
  (4, 'held_for_hedge'),
  (5, 'fv_designated')
], ['id', 'accounting_treatment']).write.format('delta').mode('overwrite').save(output_path + "/ref_accounting_treatment")

cleanup_folder(output_path+'/historical_loans')
cleanup_folder(output_path+'/ref_accounting_treatment')

# COMMAND ----------

from faker import Faker
import pandas as pd
import numpy as np

fake = Faker()

# Generate the fake data with Faker in pandas (driver-side), then create a Spark
# DataFrame. Spark Python UDFs + non-deterministic columns (F.rand) trip the
# serverless Spark Connect analyzer (spurious MISSING_GROUP_BY on write); a
# materialized createDataFrame is deterministic and serverless-safe.
_type_values = ["bonds","call","cd","credit_card","current","depreciation","internet_only","ira",
        "isa","money_market","non_product","deferred","expense","income","intangible","prepaid_card",
        "provision","reserve","suspense","tangible","non_deferred","retail_bonds","savings",
        "time_deposit","vostro","other","amortisation"]
_status_values = ["active", "cancelled", "cancelled_payout_agreed", "transactional", "other"]
_guarantee_values = ["repo", "covered_bond", "derivative", "none", "other"]
_encumbrance_values = ["be_pf", "bg_dif", "hr_di", "cy_dps", "cz_dif", "dk_gdfi", "ee_dgs", "fi_dgf", "fr_fdg",  "gb_fscs",
                       "de_edb", "de_edo", "de_edw", "gr_dgs", "hu_ndif", "ie_dgs", "it_fitd", "lv_dgf", "lt_vi",
                       "lu_fgdl", "mt_dcs", "nl_dgs", "pl_bfg", "pt_fgd", "ro_fgdb", "sk_dpf", "si_dgs", "es_fgd",
                       "se_ndo", "us_fdic"]
_purpose_values = ['admin','annual_bonus_accruals','benefit_in_kind','capital_gain_tax','cash_management','cf_hedge','ci_service',
                  'clearing','collateral','commitments','computer_and_it_cost','corporation_tax','credit_card_fee','critical_service','current_account_fee',
                  'custody','employee_stock_option','dealing_revenue','dealing_rev_deriv','dealing_rev_deriv_nse','dealing_rev_fx','dealing_rev_fx_nse',
                  'dealing_rev_sec','dealing_rev_sec_nse','deposit','derivative_fee','dividend','div_from_cis','div_from_money_mkt','donation','employee',
                  'escrow','fees','fine','firm_operating_expenses','firm_operations','fx','goodwill','insurance_fee','intra_group_fee','investment_banking_fee',
                  'inv_in_subsidiary','investment_property','interest','int_on_bond_and_frn','int_on_bridging_loan','int_on_credit_card','int_on_ecgd_lending',
                  'int_on_deposit','int_on_derivative','int_on_deriv_hedge','int_on_loan_and_adv','int_on_money_mkt','int_on_mortgage','int_on_sft','ips',
                  'loan_and_advance_fee','ni_contribution','manufactured_dividend','mortgage_fee','non_life_ins_premium','occupancy_cost','operational',
                  'operational_excess','operational_escrow','other','other_expenditure','other_fs_fee','other_non_fs_fee','other_social_contrib',
                  'other_staff_rem','other_staff_cost','overdraft_fee','own_property','pension','ppe','prime_brokerage','property','recovery',
                  'redundancy_pymt','reference','reg_loss','regular_wages','release','rent','restructuring','retained_earnings','revaluation',
                  'revenue_reserve','share_plan','staff','system','tax','unsecured_loan_fee','write_off']
# base_rate: OrderedDict-weighted pick, keeps the small None probability
_base_rate_values = ["ZERO", "UKBRBASE", "FDTR", None]
_base_rate_probs = np.array([0.5, 0.1, 0.3, 0.01]); _base_rate_probs = _base_rate_probs / _base_rate_probs.sum()

def _fake_dates(n, start, end):
  return [fake.date_time_between(start_date=start, end_date=end).strftime("%m-%d-%Y %H:%M:%S") for _ in range(n)]

def generate_transactions(num, folder, file_count, mode):
  pdf = pd.DataFrame({
    "id": range(num),
    "acc_fv_change_before_taxes": (np.random.rand(num)*1000+100).astype('int'),
    "accounting_treatment_id": (np.random.rand(num)*6).astype('int'),
    "accrued_interest": (np.random.rand(num)*100+100).astype('int'),
    "base_rate": np.random.choice(_base_rate_values, num, p=_base_rate_probs),
    "behavioral_curve_id": (np.random.rand(num)*6).astype('int'),
    "cost_center_code": [fake.country_code() for _ in range(num)],
    "country_code": [fake.country_code() for _ in range(num)],
    "date": _fake_dates(num, "-2y", "+0y"),
    "end_date": _fake_dates(num, "+0y", "+2y"),
    "next_payment_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(num)],
    "first_payment_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(num)],
    "last_payment_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(num)],
    "count": (np.random.rand(num)*500).astype('int'),
    "arrears_balance": (np.random.rand(num)*500).astype('int'),
    "balance": (np.random.rand(num)*500-30).astype('int'),
    "imit_amount": (np.random.rand(num)*500).astype('int'),
    "minimum_balance_eur": (np.random.rand(num)*500).astype('int'),
    "type": np.random.choice(_type_values, num),
    "status": np.random.choice(_status_values, num),
    "guarantee_scheme": np.random.choice(_guarantee_values, num),
    "encumbrance_type": np.random.choice(_encumbrance_values, num),
    "purpose": np.random.choice(_purpose_values, num),
  })
  (spark.createDataFrame(pdf)
    .repartition(file_count).write.format('json').mode(mode).save(folder))
  cleanup_folder(output_path+'/raw_transactions')

generate_transactions(10000, output_path+'/raw_transactions', 10, "overwrite")

# COMMAND ----------

import time
batch_count = int(dbutils.widgets.get('batch_count'))
assert batch_count <= 500, "please don't go above 500 writes, the generator will run for a too long time"
for i in range(0, int(dbutils.widgets.get('batch_count'))):
  if batch_count > 1:
    time.sleep(int(dbutils.widgets.get('batch_wait')))
  generate_transactions(int(dbutils.widgets.get('num_recs')), output_path+'/raw_transactions', 1, "append")
  print(f'Finished writing batch: {i}')
