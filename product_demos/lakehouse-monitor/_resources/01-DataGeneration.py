# Databricks notebook source
# MAGIC %md 
# MAGIC # 01-DataGeneration
# MAGIC Within this notebook, generate the following dataset:
# MAGIC
# MAGIC 1. The user bronze table, 
# MAGIC 2. The product bronze table, 
# MAGIC 3. The daily transction table

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Highlevel Overview of the Data Dictionary
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/lhm/lhm_data.png" width="600px" style="float:right"/>

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data=False)

# COMMAND ----------

if spark.catalog.tableExists('gold_user_purchase'): #and ...
  dbutils.notebook.exit(f'data alread existing in {catalog}.{dbName}. Please drop the schema to re-create them from scratch.')

# COMMAND ----------

# DBTITLE 1,Install the required library
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genearate the user table
# MAGIC
# MAGIC Schema for the User Table:
# MAGIC - **UserID**: Unique identifier for the user
# MAGIC - **Username**: User's chosen display name
# MAGIC - **Email**: User's email address
# MAGIC - **PasswordHash**: Hashed version of the user's password
# MAGIC - **FullName**: User's full name
# MAGIC - **DateOfBirth**: User's date of birth
# MAGIC - **Gender**: User's gender
# MAGIC - **PhoneNumber**: User's contact number
# MAGIC - **Address**: User's primary address
# MAGIC - **City**: User's city of residence
# MAGIC - **State**: User's state of residence
# MAGIC - **Country**: User's country of residence
# MAGIC - **PostalCode**: User's postal code
# MAGIC - **RegistrationDate**: Date when the user registered on the platform
# MAGIC - **LastLoginDate**: Date and time of the user's last login
# MAGIC - **AccountStatus**: Status of the user's account (e.g., active, suspended)
# MAGIC - **UserRole**: Role of the user (e.g., customer, admin)
# MAGIC - **PreferredPaymentMethod**: User's preferred payment method
# MAGIC - **TotalPurchaseAmount**: Total amount spent by the user
# MAGIC - **NewsletterSubscription**: Whether the user is subscribed to the newsletter (yes/no)
# MAGIC - **Wishlist**: List of product IDs in the user's wishlist
# MAGIC - **CartItems**: List of product IDs currently in the user's cart

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Function to generate user data
def generate_user_data(num_rows=10000):
    user_data = []
    
    for _ in range(num_rows):
        user = {
            "UserID": fake.uuid4(),
            "Username": fake.user_name(),
            "Email": fake.email(),
            "PasswordHash": fake.sha256(),
            "FullName": fake.name(),
            "DateOfBirth": fake.date_of_birth(minimum_age=18, maximum_age=90),
            "Gender": random.choice(["Male", "Female", "Other"]),
            "PhoneNumber": fake.phone_number(),
            "Address": fake.address(),
            "City": fake.city(),
            "State": fake.state(),
            "Country": fake.country(),
            "PostalCode": fake.postcode(),
            "RegistrationDate": fake.date_this_decade(),
            "LastLoginDate": fake.date_time_between(start_date="-1y", end_date="now"),
            "AccountStatus": random.choice(["Active", "Suspended", "Inactive"]),
            "UserRole": random.choice(["Customer", "Admin"]),
            "PreferredPaymentMethod": random.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer"]),
            "TotalPurchaseAmount": round(random.uniform(0, 10000), 2),
            "NewsletterSubscription": random.choice([True, False]),
            "Wishlist": [fake.uuid4() for _ in range(random.randint(0, 10))],
            "CartItems": [fake.uuid4() for _ in range(random.randint(0, 5))]
        }
        user_data.append(user)
    
    return pd.DataFrame(user_data)

# Generate the user data
user_df = generate_user_data(10000)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, BooleanType, ArrayType
# Convert the Pandas DataFrame to a PySpark DataFrame
schema = StructType([
    StructField("UserID", StringType(), False),
    StructField("Username", StringType(), False),
    StructField("Email", StringType(), False),
    StructField("PasswordHash", StringType(), False),
    StructField("FullName", StringType(), False),
    StructField("DateOfBirth", DateType(), False),
    StructField("Gender", StringType(), False),
    StructField("PhoneNumber", StringType(), False),
    StructField("Address", StringType(), False),
    StructField("City", StringType(), False),
    StructField("State", StringType(), False),
    StructField("Country", StringType(), False),
    StructField("PostalCode", StringType(), False),
    StructField("RegistrationDate", DateType(), False),
    StructField("LastLoginDate", DateType(), False),
    StructField("AccountStatus", StringType(), False),
    StructField("UserRole", StringType(), False),
    StructField("PreferredPaymentMethod", StringType(), False),
    StructField("TotalPurchaseAmount", FloatType(), False),
    StructField("NewsletterSubscription", BooleanType(), False),
    StructField("Wishlist", ArrayType(StringType()), False),
    StructField("CartItems", ArrayType(StringType()), False)
])

# Create Spark DataFrame
spark_user_df = spark.createDataFrame(user_df, schema)

# Write the Spark DataFrame to Delta format
spark_user_df.write.mode('overwrite').saveAsTable('bronze_user')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genearate the product table
# MAGIC
# MAGIC Schema for the Product Table:
# MAGIC - **ProductID**: Unique identifier for the product
# MAGIC - **ProductName**: Name of the product
# MAGIC - **Category**: Category to which the product belongs
# MAGIC - **SubCategory**: Subcategory of the product
# MAGIC - **Brand**: Brand of the product
# MAGIC - **Description**: Detailed description of the product
# MAGIC - **Price**: Price of the product
# MAGIC - **Discount**: Discount on the product (if any)
# MAGIC - **StockQuantity**: Number of items available in stock
# MAGIC - **SKU**: Stock Keeping Unit identifier
# MAGIC - **ProductImageURL**: URL of the product image
# MAGIC - **ProductRating**: Average rating of the product
# MAGIC - **NumberOfReviews**: Number of reviews for the product
# MAGIC - **SupplierID**: Unique identifier for the supplier
# MAGIC - **DateAdded**: Date when the product was added to the inventory
# MAGIC - **Dimensions**: Dimensions of the product (L x W x H)
# MAGIC - **Weight**: Weight of the product
# MAGIC - **Color**: Color of the product
# MAGIC - **Material**: Material of the product
# MAGIC - **WarrantyPeriod**: Warranty period of the product
# MAGIC - **ReturnPolicy**: Return policy for the product
# MAGIC - **ShippingCost**: Cost of shipping the product
# MAGIC - **ProductTags**: Tags associated with

# COMMAND ----------

from faker import Faker
import pandas as pd
import random

# Initialize Faker
fake = Faker()

# Expanded list of realistic product names related to categories
product_names = {
    "Electronics": [
        "Smartphone", "Laptop", "Tablet", "Desktop Computer", "Camera", "Headphones", "Speakers", "Smartwatch",
        "Fitness Tracker", "Bluetooth Earbuds", "Gaming Console", "Television"
    ],
    "Clothing": [
        "Running Shoes", "Hiking Boots", "Sneakers", "Sandals", "Slippers", "Formal Shoes", "Wrist Watch",
        "Sunglasses", "Handbag", "Backpack", "T-Shirt", "Sweater", "Jacket", "Jeans", "Dress", "Skirt", "Shorts",
        "Swimwear", "Hat", "Scarf"
    ],
    "Home & Kitchen": [
        "Vacuum Cleaner", "Blender", "Microwave Oven", "Refrigerator", "Air Conditioner", "Heater", "Fan",
        "Electric Kettle", "Coffee Maker", "Toaster", "Cookware Set", "Knife Set", "Cutting Board"
    ],
    "Books": [
        "Cookbook", "Novel", "Textbook", "Journal", "Notebook", "Children's Book"
    ],
    "Toys": [
        "Puzzle", "Board Game", "Action Figure", "Doll", "Toy Car", "Building Blocks"
    ],
    "Sports": [
        "Bicycle", "Treadmill", "Dumbbells", "Yoga Mat", "Protein Powder"
    ],
    "Health & Beauty": [
        "Skincare Set", "Shampoo", "Conditioner", "Hair Dryer", "Electric Toothbrush"
    ]
}

subcategories = {
    "Electronics": ["Smartphones", "Laptops", "Cameras", "Headphones", "Speakers"],
    "Clothing": ["Men", "Women", "Kids", "Accessories", "Footwear"],
    "Home & Kitchen": ["Appliances", "Cookware", "Furniture", "Decor", "Bedding"],
    "Books": ["Fiction", "Non-Fiction", "Children's Books", "Educational", "Mystery"],
    "Toys": ["Educational Toys", "Action Figures", "Board Games", "Dolls", "Puzzles"],
    "Sports": ["Fitness Equipment", "Outdoor Gear", "Team Sports", "Individual Sports", "Sportswear"],
    "Health & Beauty": ["Skincare", "Makeup", "Supplements", "Haircare", "Personal Care"]
}

# Expanded list of realistic brand names
brands = [
    "BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF", "BrandG", "BrandH", "BrandI", "BrandJ",
    "BrandK", "BrandL", "BrandM", "BrandN", "BrandO", "BrandP", "BrandQ", "BrandR", "BrandS", "BrandT",
    "BrandU", "BrandV", "BrandW", "BrandX", "BrandY", "BrandZ", "BrandAA", "BrandBB", "BrandCC", "BrandDD",
    "BrandEE", "BrandFF", "BrandGG", "BrandHH", "BrandII", "BrandJJ", "BrandKK", "BrandLL", "BrandMM", "BrandNN",
    "BrandOO", "BrandPP", "BrandQQ", "BrandRR", "BrandSS", "BrandTT", "BrandUU", "BrandVV", "BrandWW", "BrandXX"
]

# Category-specific descriptions
descriptions = {
    "Electronics": [
        "Latest technology with cutting-edge features.",
        "High performance and sleek design.",
        "Ideal for tech enthusiasts and professionals.",
        "Reliable and durable with excellent battery life.",
        "Compact and lightweight for easy portability."
    ],
    "Clothing": [
        "Comfortable and stylish for any occasion.",
        "Made from high-quality materials for a perfect fit.",
        "Trendy design that stands out.",
        "Versatile and easy to pair with different outfits.",
        "Durable fabric for long-lasting wear."
    ],
    "Home & Kitchen": [
        "Essential appliance for modern homes.",
        "Stylish design to complement your kitchen.",
        "Energy-efficient and easy to use.",
        "High performance for all your cooking needs.",
        "Compact design saves space."
    ],
    "Books": [
        "Engaging story that captivates readers.",
        "Informative and educational content.",
        "Perfect for readers of all ages.",
        "Beautifully illustrated with vibrant colors.",
        "Thought-provoking and inspiring."
    ],
    "Toys": [
        "Fun and educational for children.",
        "Safe and durable materials.",
        "Encourages creativity and imagination.",
        "Perfect gift for kids of all ages.",
        "Bright and colorful design."
    ],
    "Sports": [
        "High-performance gear for athletes.",
        "Durable and lightweight materials.",
        "Designed for comfort and efficiency.",
        "Ideal for both beginners and professionals.",
        "Enhances your performance in sports."
    ],
    "Health & Beauty": [
        "Nourishes and revitalizes your skin.",
        "High-quality ingredients for best results.",
        "Suitable for all skin types.",
        "Enhances your natural beauty.",
        "Gentle and effective formula."
    ]
}

# Function to generate product data
def generate_product_data(num_rows=10000):
    product_data = []
    
    for _ in range(num_rows):
        category = random.choice(list(subcategories.keys()))
        product = {
            "ProductID": fake.uuid4(),
            "ProductName": random.choice(product_names[category]),
            "Category": category,
            "SubCategory": random.choice(subcategories[category]),
            "Brand": random.choice(brands),
            "Description": random.choice(descriptions[category]),
            "Price": round(random.uniform(5, 2000), 2),
            "Discount": round(random.uniform(0, 0.5), 2),  # Discount as a fraction
            "StockQuantity": random.randint(0, 1000),
            "SKU": fake.bothify(text='???-########'),
            "ProductImageURL": fake.image_url(),
            "ProductRating": round(random.uniform(1, 5), 1),
            "NumberOfReviews": random.randint(0, 5000),
            "SupplierID": fake.uuid4(),
            "DateAdded": fake.date_this_decade(),
            "Dimensions": f"{random.uniform(1, 100):.2f} x {random.uniform(1, 100):.2f} x {random.uniform(1, 100):.2f}",
            "Weight": round(random.uniform(0.1, 50), 2),
            "Color": fake.color_name(),
            "Material": random.choice(["Plastic", "Metal", "Wood", "Glass", "Fabric"]),
            "WarrantyPeriod": f"{random.randint(1, 24)} months",
            "ReturnPolicy": random.choice(["30 days", "60 days", "No returns"]),
            "ShippingCost": round(random.uniform(0, 50), 2),
            "ProductTags": [fake.word() for _ in range(random.randint(1, 5))]
        }
        product_data.append(product)
    
    return pd.DataFrame(product_data)

# Generate the product data
product_df = generate_product_data(10000)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType, ArrayType

# Convert the Pandas DataFrame to a PySpark DataFrame
schema = StructType([
    StructField("ProductID", StringType(), False),
    StructField("ProductName", StringType(), False),
    StructField("Category", StringType(), False),
    StructField("SubCategory", StringType(), False),
    StructField("Brand", StringType(), False),
    StructField("Description", StringType(), False),
    StructField("Price", FloatType(), False),
    StructField("Discount", FloatType(), False),
    StructField("StockQuantity", IntegerType(), False),
    StructField("SKU", StringType(), False),
    StructField("ProductImageURL", StringType(), False),
    StructField("ProductRating", FloatType(), False),
    StructField("NumberOfReviews", IntegerType(), False),
    StructField("SupplierID", StringType(), False),
    StructField("DateAdded", DateType(), False),
    StructField("Dimensions", StringType(), False),
    StructField("Weight", FloatType(), False),
    StructField("Color", StringType(), False),
    StructField("Material", StringType(), False),
    StructField("WarrantyPeriod", StringType(), False),
    StructField("ReturnPolicy", StringType(), False),
    StructField("ShippingCost", FloatType(), False),
    StructField("ProductTags", ArrayType(StringType()), False)
])

# Create Spark DataFrame
spark_product_df = spark.createDataFrame(product_df, schema)

# Write the Spark DataFrame to Delta format
spark_product_df.write.mode('overwrite').saveAsTable('bronze_product')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genearate the transactions table
# MAGIC
# MAGIC Schema for the transactions Table:
# MAGIC - **TransactionID**: Unique identifier for the transcation
# MAGIC - **UserID**: Unique identifier for the user
# MAGIC - **ProductID**: Unique identifier for the product
# MAGIC - **TransactionDate**: Transcation timestamp
# MAGIC - **Quantity**: Number of the items ordered
# MAGIC - **UnitPrice**: Unit price for the item ordered
# MAGIC - **TotalPrice**: Total amount of the purchase
# MAGIC - **PaymentMethod**: The payment method
# MAGIC - **ShippingAddress**: The shipping address for the order
# MAGIC - **LoyaltyPointsEarned**: The loyalty points earned per purchase (10% of total amount)
# MAGIC - **GiftWrap**: yes or no on gift wrap
# MAGIC - **SpecialInstructions**: Any other special intructions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customizations:
# MAGIC - Date Range: Transactions are generated for each day within the specified date range.
# MAGIC - Seasonality: Different seasons have different base transaction volumes.
# MAGIC - Weekday/Weekend: Weekend transaction volumes are higher than weekdays.
# MAGIC - Marketing Campaigns: Specific days can have higher transaction volumes due to marketing campaigns.

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Function to generate transaction data
def generate_transaction_data(user_df, product_df, start_date, end_date, campaigns={}):
    transaction_data = []
    
    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Define seasonality factors
    seasonality_factors = {
        "winter": 1.6,
        "spring": 1.1,
        "summer": 1.2,
        "autumn": 1.4
    }
    
    # Iterate over each date in the range
    current_date = start_date
    while current_date <= end_date:
        # Determine seasonality factor based on month
        month = current_date.month
        if month in [12, 1]:
            seasonality_factor = seasonality_factors["winter"]
        elif month in [2, 3, 4, 5]:
            seasonality_factor = seasonality_factors["spring"]
        elif month in [6, 7, 8]:
            seasonality_factor = seasonality_factors["summer"]
        else:
            seasonality_factor = seasonality_factors["autumn"]
        
        # Determine weekday/weekend factor
        if current_date.weekday() < 5:  # Weekday
            day_factor = 1.0
        else:  # Weekend
            day_factor = 1.3
        
        # Determine marketing campaign factor
        campaign_factor = campaigns.get(current_date.strftime("%Y-%m-%d"), 1.0)
        
        # Calculate the base number of transactions for the day
        base_transactions = int(100 * day_factor * seasonality_factor * campaign_factor)
        
        # Apply a random multiplier to introduce variability
        random_multiplier = random.uniform(0.9, 1.1)  # Adjust the range for desired variability
        daily_transactions = int(base_transactions * random_multiplier)
        
        # Generate transactions for the day
        for _ in range(daily_transactions):
            user = user_df.sample(1).iloc[0]
            product = product_df.sample(1).iloc[0]
            quantity = random.randint(1, 5)
            transaction = {
                "TransactionID": fake.uuid4(),
                "UserID": user["UserID"],
                "ProductID": product["ProductID"],
                "TransactionDate": fake.date_time_between(start_date=current_date, end_date=current_date + timedelta(days=1)),
                "Quantity": quantity,
                "UnitPrice": product["Price"],
                "TotalPrice": round(product["Price"] * quantity, 2),
                "PaymentMethod": random.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer"]),
                "ShippingAddress": user["Address"],
                "LoyaltyPointsEarned": int(round(product["Price"] * quantity * 0.1)),  # Example: 10% of the total price in loyalty points
                "GiftWrap": random.choice(["yes", "no"]),
                "SpecialInstructions": fake.sentence() if random.choice([True, False]) else ""
            }
            transaction_data.append(transaction)
        
        # Move to the next day
        current_date += timedelta(days=1)
    
    return pd.DataFrame(transaction_data)

# Generate the transaction data
# Set the current date
current_date = datetime.now()

# Calculate the start date (1.5 years ago)
start_date = (current_date - timedelta(days=365 * 1.5)).strftime("%Y-%m-%d")

# Set the end date to today
end_date = current_date.strftime("%Y-%m-%d")

# Define the campaigns with the last campaign date being 8 days before today
campaigns = {
    # "2023-07-15": 2.0 ,  # Example campaign day with doubled sales
    # "2023-11-23": 1.5 ,  # Another example campaign day with 50% higher sales
    # "2024-03-10": 2.0 , # Example campaign day with doubled sales
    (current_date - timedelta(days=1)).strftime("%Y-%m-%d"): 2.0  # Last campaign date is today - 1 days
}

# Generate the transaction data
transaction_df = generate_transaction_data(user_df, product_df, start_date, end_date, campaigns)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType

# Convert the Pandas DataFrame to a PySpark DataFrame
schema = StructType([
    StructField("TransactionID", StringType(), False),
    StructField("UserID", StringType(), False),
    StructField("ProductID", StringType(), False),
    StructField("TransactionDate", TimestampType(), False),
    StructField("Quantity", IntegerType(), False),
    StructField("UnitPrice", FloatType(), False),
    StructField("TotalPrice", FloatType(), False),
    StructField("PaymentMethod", StringType(), False),
    StructField("ShippingAddress", StringType(), False),
    StructField("LoyaltyPointsEarned", IntegerType(), False),
    StructField("GiftWrap", StringType(), False),
    StructField("SpecialInstructions", StringType(), False)
])

# Create Spark DataFrame
spark_transaction_df = spark.createDataFrame(transaction_df, schema)

# Write the Spark DataFrame to Delta format
spark_transaction_df.write.mode('overwrite').saveAsTable('bronze_transaction')

# COMMAND ----------

# Join the DataFrames
joined_df = transaction_df.merge(user_df, on="UserID", how="left").merge(product_df, on="ProductID", how="left")

# COMMAND ----------

import pandas as pd
import numpy as np
import random
from datetime import datetime

def inject_issues(df, campaign_start_dates):
    # Convert campaign_start_dates to datetime
    campaign_start_dates = pd.to_datetime(campaign_start_dates)
    
    # Ensure TransactionDate is in datetime format
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
    
    # Create a temporary date column from TransactionDate
    df['TempDate'] = pd.to_datetime(df['TransactionDate'].dt.date)
    
    # Create a Campaign_flag column
    df['Campaign_flag'] = False
    
    # Nulls spike in PreferredPaymentMethod from 20% to 40% in May 2024
    # may_2024_mask = (df['TempDate'].dt.year == 2024) & (df['TempDate'].dt.month == 5)
    # preferred_payment_method_null_indices = df[may_2024_mask].sample(frac=0.48).index
    # df.loc[preferred_payment_method_null_indices, 'PreferredPaymentMethod'] = np.nan
    
    # Steady nulls around 10-15% in specified columns
    steady_null_columns = ['ProductTags', 'ShippingAddress', 'Wishlist', 'GiftWrap']
    for column in steady_null_columns:
        null_indices = df.sample(frac=random.uniform(0.1, 0.15)).index
        df.loc[null_indices, column] = np.nan

    # Steady nulls around 10% in PreferredPaymentMethod columns
    steady_null_columns2 = ['PreferredPaymentMethod']
    for column in steady_null_columns2:
        null_indices = df.sample(frac=random.uniform(0.05, 0.09)).index
        df.loc[null_indices, column] = np.nan
    
    
    # 60% zeros in Discount distributed evenly over time
    discount_zero_indices = df.sample(frac=0.6).index
    df.loc[discount_zero_indices, 'Discount'] = 0
    
    # 10% zeros in ProductRating distributed evenly over time
    product_rating_zero_indices = df.sample(frac=0.1).index
    df.loc[product_rating_zero_indices, 'ProductRating'] = 0
    
    # NumberOfReviews drops to 5% in sync with marketing campaigns for 10 days
    for start_date in campaign_start_dates:
        campaign_mask = (df['TempDate'] >= start_date) & (df['TempDate'] < start_date + pd.Timedelta(days=10))
        number_of_reviews_zero_indices_campaign = df[campaign_mask].sample(frac=0.05).index
        df.loc[number_of_reviews_zero_indices_campaign, 'NumberOfReviews'] = 0
        preferred_payment_method_null_indices = df[campaign_mask].sample(frac=0.48).index
        df.loc[preferred_payment_method_null_indices, 'PreferredPaymentMethod'] = np.nan
        applepay_indices = df[campaign_mask].sample(frac=0.8).index
        df.loc[applepay_indices, 'PaymentMethod'] = 'Apple Pay'
    
    steady_reviews_zero_indices = df[~df.index.isin(number_of_reviews_zero_indices_campaign)].sample(frac=random.uniform(0.2, 0.3)).index
    df.loc[steady_reviews_zero_indices, 'NumberOfReviews'] = 0
    
    # Introduce ApplePay to PaymentMethod after May 2024
    # applepay_mask = (df['TempDate'].dt.year == 2024) & (df['TempDate'].dt.month >= 5)
    # applepay_indices = df[applepay_mask].sample(frac=0.8).index
    # df.loc[applepay_indices, 'PaymentMethod'] = 'Apple Pay'
    
    # Overwrite over 50% of WarrantyPeriod to be '15 days' after May 2024
    warranty_period_mask = (df['TempDate'].dt.year == 2024) & (df['TempDate'].dt.month >= 5)
    warranty_period_indices = df[warranty_period_mask].sample(frac=0.7).index
    df.loc[warranty_period_indices, 'WarrantyPeriod'] = '15 days'
    
    # Overwrite over 50% of ReturnPolicy to be 'no returns' after May 2024
    return_policy_mask = (df['TempDate'].dt.year == 2024) & (df['TempDate'].dt.month >= 5)
    return_policy_indices = df[return_policy_mask].sample(frac=0.7).index
    df.loc[return_policy_indices, 'ReturnPolicy'] = 'no returns'
    
    # Dramatic change in Quantity and TotalPrice for 10 days after each campaign start date
    for start_date in campaign_start_dates:
        campaign_effect_mask = (df['TempDate'] >= start_date) & (df['TempDate'] < start_date + pd.Timedelta(days=10))
        df.loc[campaign_effect_mask, 'Quantity'] = df.loc[campaign_effect_mask, 'Quantity'] * 1.5
        df.loc[campaign_effect_mask, 'TotalPrice'] = df.loc[campaign_effect_mask, 'Quantity'] * df.loc[campaign_effect_mask, 'UnitPrice']
        
        # Set the Campaign_flag for these dates
        df.loc[campaign_effect_mask, 'Campaign_flag'] = True
    
    # Drop the temporary date column
    df.drop(columns=['TempDate'], inplace=True)
    
    return df

# Example usage
campaign_start_dates = [(current_date - timedelta(days=1)).strftime("%Y-%m-%d")]
#campaign_start_dates = ["2023-07-15", "2023-11-23", "2024-03-10", (current_date - timedelta(days=1)).strftime("%Y-%m-%d")]
joined_df_with_issues = inject_issues(joined_df, campaign_start_dates)


# COMMAND ----------

# Convert pandas DataFrame to Spark DataFrame
spark_joined_df_with_issues = spark.createDataFrame(joined_df_with_issues)

# Write the Spark DataFrame to Delta format
spark_joined_df_with_issues.write.option("mergeSchema", "true").mode('overwrite').saveAsTable('silver_transaction')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Generate Gold Tables

# COMMAND ----------

from pyspark.sql import functions as F
# Create a temporary column for Month
spark_joined_df_with_issues = spark_joined_df_with_issues.withColumn("Month", F.date_format(F.col("TransactionDate"), "yyyy-MM"))

# Monthly Sales Summary by Category
monthly_sales_summary = spark_joined_df_with_issues \
    .groupBy("Month", "Category") \
    .agg(
        F.sum("TotalPrice").alias("TotalSales"),
        F.sum("Quantity").alias("TotalQuantitySold")
    ) \
    .orderBy("Month", "Category")

# Write the Spark DataFrame to Delta format
monthly_sales_summary.write.mode('overwrite').option("mergeSchema", "true").mode('overwrite').saveAsTable(f'gold_monthly_sales')

# COMMAND ----------

from pyspark.sql import Window
# Top 10 Products by Total Sales by Month
top_10_products_by_month = spark_joined_df_with_issues \
    .groupBy("Month", "ProductID", "ProductName") \
    .agg(
        F.sum("TotalPrice").alias("TotalSales")
    ) \
    .withColumn("Rank", F.row_number().over(Window.partitionBy("Month").orderBy(F.desc("TotalSales")))) \
    .filter(F.col("Rank") <= 10) \
    .orderBy("Month", "Rank")

# Write the Spark DataFrame to Delta format
top_10_products_by_month.write.mode('overwrite').option("mergeSchema", "true").mode('overwrite').saveAsTable('gold_top_products')


# COMMAND ----------

# User Purchase Behavior by Month
user_purchase_behavior_by_month = spark_joined_df_with_issues \
    .groupBy("Month", "UserID", "Username") \
    .agg(
        F.sum("TotalPrice").alias("TotalPurchaseAmount"),
        F.avg("TotalPrice").alias("AveragePurchaseAmount"),
        F.count("TransactionID").alias("TotalTransactions")
    ) \
    .orderBy("Month", "UserID")

# Write the Spark DataFrame to Delta format
user_purchase_behavior_by_month.write.mode('overwrite').option("mergeSchema", "true").mode('overwrite').saveAsTable('gold_user_purchase')
