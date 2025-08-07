# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

spark.sql(f'use catalog {catalog}')
spark.sql(f'use schema {dbName}')

# COMMAND ----------

"""
Telco Data Generator
This script generates realistic telco data including customers, subscriptions, and billing information.
"""

import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Any

import pandas as pd
from faker import Faker


class TelcoDataGenerator:
    """Generator for telco data including customers, subscriptions, and billing."""
    
    def __init__(self, seed: int = 42):
        """Initialize the generator with a seed for reproducibility."""
        self.fake = Faker()
        Faker.seed(seed)
        self.random = random
        self.random.seed(seed)
        
        # ID counters
        self.customer_id_counter = 10001
        self.subscription_id_counter = 20001
        self.billing_id_counter = 30001
        
    def generate_customers(self, count: int = 1000) -> pd.DataFrame:
        """Generate customer data with realistic distributions."""
        
        customer_segments = {
            "Individual": 0.45, "Family": 0.25, "Business": 0.20, 
            "Premium": 0.08, "Student": 0.02
        }
        
        customer_statuses = {
            "Active": 0.85, "Inactive": 0.10, "Suspended": 0.03, "Churned": 0.02
        }
        
        data = []
        
        for _ in range(count):
            customer_id = self.customer_id_counter
            self.customer_id_counter += 1
            
            customer_segment = self.random.choices(
                list(customer_segments.keys()), weights=list(customer_segments.values())
            )[0]
            
            # Generate personal info
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            email = self.fake.email()
            phone = self.fake.phone_number()
            address = self.fake.street_address()
            city = self.fake.city()
            state = self.fake.state_abbr()
            zip_code = self.fake.zipcode()
            
            # Registration date (skewed towards recent)
            if self.random.random() < 0.70:
                registration_date = self.fake.date_between(start_date='-3y', end_date='today')
            elif self.random.random() < 0.95:
                registration_date = self.fake.date_between(start_date='-8y', end_date='-3y')
            else:
                registration_date = self.fake.date_between(start_date='-15y', end_date='-8y')
            
            customer_status = self.random.choices(
                list(customer_statuses.keys()), weights=list(customer_statuses.values())
            )[0]
            
            # Calculate metrics
            tenure_years = (date.today() - registration_date).days / 365.25
            
            # Loyalty tier
            if customer_segment in ["Premium", "Business"] and tenure_years > 2:
                loyalty_tier = self.random.choices(["Gold", "Platinum"], weights=[0.6, 0.4])[0]
            elif tenure_years > 5:
                loyalty_tier = self.random.choices(["Silver", "Gold"], weights=[0.7, 0.3])[0]
            else:
                loyalty_tier = self.random.choices(["Bronze", "Silver"], weights=[0.8, 0.2])[0]
            
            # Churn risk
            churn_risk = self.random.randint(10, 80)
            if customer_segment in ["Premium", "Business"]:
                churn_risk -= 15
            if loyalty_tier in ["Gold", "Platinum"]:
                churn_risk -= 10
            if tenure_years > 3:
                churn_risk -= min(20, int(tenure_years * 5))
            if customer_status != "Active":
                churn_risk += 30
            churn_risk = max(1, min(100, churn_risk))
            
            # Customer value
            base_value = {"Individual": 40, "Family": 60, "Business": 75, "Premium": 85, "Student": 30}[customer_segment]
            tier_bonus = {"Bronze": 0, "Silver": 10, "Gold": 20, "Platinum": 30}[loyalty_tier]
            tenure_bonus = min(15, int(tenure_years * 3))
            customer_value = base_value + tier_bonus + tenure_bonus + self.random.randint(-10, 10)
            customer_value = max(1, min(100, customer_value))
            
            data.append({
                'customer_id': customer_id,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'address': address,
                'city': city,
                'state': state,
                'zip_code': zip_code,
                'customer_segment': customer_segment,
                'registration_date': registration_date,
                'customer_status': customer_status,
                'loyalty_tier': loyalty_tier,
                'tenure_years': round(tenure_years, 2),
                'churn_risk_score': churn_risk,
                'customer_value_score': customer_value
            })
        
        return pd.DataFrame(data)
    
    def generate_subscriptions(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """Generate subscription data for customers."""
        
        service_types = {
            "Mobile": 0.60, "Internet": 0.25, "Fiber": 0.10, "ADSL": 0.05
        }
        
        subscription_statuses = {
            "Active": 0.80, "Suspended": 0.10, "Cancelled": 0.08, "Pending": 0.02
        }
        
        plan_tiers = {
            "Basic": 0.40, "Standard": 0.35, "Premium": 0.20, "Unlimited": 0.05
        }
        
        data = []
        
        for _, customer in customers_df.iterrows():
            customer_id = customer['customer_id']
            customer_segment = customer['customer_segment']
            
            # Number of subscriptions per customer
            if customer_segment in ["Business", "Premium"]:
                if self.random.random() < 0.3:
                    subscription_count = 1
                elif self.random.random() < 0.7:
                    subscription_count = 2
                else:
                    subscription_count = self.random.randint(3, 5)
            else:
                if self.random.random() < 0.7:
                    subscription_count = 1
                elif self.random.random() < 0.9:
                    subscription_count = 2
                else:
                    subscription_count = 3
            
            for _ in range(subscription_count):
                subscription_id = self.subscription_id_counter
                self.subscription_id_counter += 1
                
                service_type = self.random.choices(
                    list(service_types.keys()), weights=list(service_types.values())
                )[0]
                
                plan_tier = self.random.choices(
                    list(plan_tiers.keys()), weights=list(plan_tiers.values())
                )[0]
                
                plan_name = f"{plan_tier} {service_type} Plan"
                
                # Pricing
                base_prices = {
                    "Mobile": {"Basic": 30, "Standard": 50, "Premium": 80, "Unlimited": 120},
                    "Internet": {"Basic": 40, "Standard": 60, "Premium": 90, "Unlimited": 150},
                    "Fiber": {"Basic": 60, "Standard": 80, "Premium": 120, "Unlimited": 200},
                    "ADSL": {"Basic": 25, "Standard": 35, "Premium": 50, "Unlimited": 80}
                }
                
                base_price = base_prices[service_type][plan_tier]
                monthly_charge = base_price + self.random.randint(-5, 10)
                monthly_charge = max(20, monthly_charge)
                
                # Start date
                registration_date = customer['registration_date']
                days_since_reg = (date.today() - registration_date).days
                
                if days_since_reg > 0:
                    random_days = int(self.random.betavariate(2, 5) * days_since_reg)
                    start_date = registration_date + timedelta(days=random_days)
                else:
                    start_date = registration_date
                
                # Contract length
                if service_type in ["Fiber", "Internet"]:
                    contract_lengths = [12, 24, 36]
                    contract_weights = [0.3, 0.6, 0.1]
                else:
                    contract_lengths = [0, 12, 24]
                    contract_weights = [0.4, 0.4, 0.2]
                
                contract_length = self.random.choices(
                    contract_lengths, weights=contract_weights
                )[0]
                
                status = self.random.choices(
                    list(subscription_statuses.keys()), weights=list(subscription_statuses.values())
                )[0]
                
                autopay_enabled = self.random.random() < 0.7 if status == "Active" else self.random.random() < 0.3
                
                data.append({
                    'subscription_id': subscription_id,
                    'customer_id': customer_id,
                    'service_type': service_type,
                    'plan_name': plan_name,
                    'plan_tier': plan_tier,
                    'monthly_charge': monthly_charge,
                    'start_date': start_date,
                    'contract_length_months': contract_length,
                    'status': status,
                    'autopay_enabled': autopay_enabled
                })
        
        return pd.DataFrame(data)
    
    def generate_billing(self, subscriptions_df: pd.DataFrame) -> pd.DataFrame:
        """Generate billing data for subscriptions."""
        
        payment_statuses = {
            "Paid": 0.75, "Unpaid": 0.15, "Late": 0.08, "Partial": 0.02
        }
        
        payment_methods = {
            "Credit Card": 0.50, "Bank Transfer": 0.25, "PayPal": 0.15, "Check": 0.10
        }
        
        data = []
        
        # Generate billing for the last 12 months
        end_date = date.today()
        start_date = end_date - timedelta(days=365)
        
        for _, subscription in subscriptions_df.iterrows():
            subscription_id = subscription['subscription_id']
            customer_id = subscription['customer_id']
            monthly_charge = subscription['monthly_charge']
            start_date_sub = subscription['start_date']
            status = subscription['status']
            
            if status in ["Cancelled", "Pending"]:
                continue
            
            current_date = start_date
            while current_date <= end_date:
                if current_date < start_date_sub:
                    current_date += timedelta(days=30)
                    continue
                
                billing_id = self.billing_id_counter
                self.billing_id_counter += 1
                
                billing_date = date(current_date.year, current_date.month, 1)
                due_date = date(current_date.year, current_date.month, 15)
                
                base_amount = monthly_charge
                additional_charges = 0.0
                if self.random.random() < 0.2:
                    additional_charges = round(self.random.uniform(5.0, 30.0), 2)
                
                tax_rate = self.random.uniform(0.08, 0.12)
                pre_tax = base_amount + additional_charges
                tax_amount = round(pre_tax * tax_rate, 2)
                total_amount = round(pre_tax + tax_amount, 2)
                
                # Payment status
                months_ago = (end_date - billing_date).days // 30
                if months_ago > 2:
                    adjusted_statuses = {
                        "Paid": payment_statuses["Paid"] + 0.15,
                        "Unpaid": max(0.01, payment_statuses["Unpaid"] - 0.1),
                        "Late": max(0.01, payment_statuses["Late"] - 0.03),
                        "Partial": max(0.01, payment_statuses["Partial"] - 0.02)
                    }
                    total = sum(adjusted_statuses.values())
                    adjusted_statuses = {k: v/total for k, v in adjusted_statuses.items()}
                    payment_status = self.random.choices(
                        list(adjusted_statuses.keys()), weights=list(adjusted_statuses.values())
                    )[0]
                else:
                    payment_status = self.random.choices(
                        list(payment_statuses.keys()), weights=list(payment_statuses.values())
                    )[0]
                
                # Payment details
                payment_amount = None
                payment_date = None
                payment_method = None
                
                if payment_status == "Paid":
                    payment_amount = total_amount
                    if self.random.random() < 0.9:
                        days_after_billing = self.random.randint(1, 14)
                        payment_date = billing_date + timedelta(days=days_after_billing)
                    else:
                        days_late = self.random.randint(1, 5)
                        payment_date = due_date + timedelta(days=days_late)
                    payment_method = self.random.choices(
                        list(payment_methods.keys()), weights=list(payment_methods.values())
                    )[0]
                
                elif payment_status == "Partial":
                    payment_amount = round(total_amount * self.random.uniform(0.3, 0.7), 2)
                    days_late = self.random.randint(1, 10)
                    payment_date = due_date + timedelta(days=days_late)
                    payment_method = self.random.choices(
                        list(payment_methods.keys()), weights=list(payment_methods.values())
                    )[0]
                
                elif payment_status == "Late":
                    if self.random.random() < 0.7:
                        payment_amount = total_amount
                        days_late = self.random.randint(5, 20)
                        payment_date = due_date + timedelta(days=days_late)
                        payment_method = self.random.choices(
                            list(payment_methods.keys()), weights=list(payment_methods.values())
                        )[0]
                
                data.append({
                    'billing_id': billing_id,
                    'subscription_id': subscription_id,
                    'customer_id': customer_id,
                    'billing_date': billing_date,
                    'due_date': due_date,
                    'base_amount': base_amount,
                    'additional_charges': additional_charges,
                    'tax_amount': tax_amount,
                    'total_amount': total_amount,
                    'payment_amount': payment_amount,
                    'payment_date': payment_date,
                    'payment_method': payment_method,
                    'payment_status': payment_status,
                    'billing_cycle': billing_date.strftime("%Y-%m")
                })
                
                if current_date.month == 12:
                    current_date = date(current_date.year + 1, 1, 1)
                else:
                    current_date = date(current_date.year, current_date.month + 1, 1)
        
        return pd.DataFrame(data)

# COMMAND ----------

spark.sql(f'CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}')

# COMMAND ----------

"""Main function to generate all data."""
print("Generating telco data...")

generator = TelcoDataGenerator(seed=42)

# Generate data
print("Generating customers...")
customers_df = generator.generate_customers(count=1000)

print("Generating subscriptions...")
subscriptions_df = generator.generate_subscriptions(customers_df)

print("Generating billing records...")
billing_df = generator.generate_billing(subscriptions_df)

path = f'/Volumes/{catalog}/{schema}/{volume_name}'
dbutils.fs.mkdirs(f'{path}/customers')
dbutils.fs.mkdirs(f'{path}/subscriptions')
dbutils.fs.mkdirs(f'{path}/billing')

customers_spark_df = spark.createDataFrame(customers_df)
subscriptions_spark_df = spark.createDataFrame(subscriptions_df)
billing_spark_df = spark.createDataFrame(billing_df)

# Save as tables
customers_spark_df.repartition(1).write.format("parquet").mode("overwrite").save(f"/Volumes/{catalog}/{schema}/raw_data/customers")
subscriptions_spark_df.repartition(1).write.format("parquet").mode("overwrite").save(f"/Volumes/{catalog}/{schema}/raw_data/subscriptions")
billing_spark_df.repartition(1).write.format("parquet").mode("overwrite").save(f"/Volumes/{catalog}/{schema}/raw_data/billing")

#customers_spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.customers")
#subscriptions_spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.subscriptions")
#billing_spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.billing")

print("\nData generation complete!")
print(f"Raw data saved: customers, subscriptions, billing")


