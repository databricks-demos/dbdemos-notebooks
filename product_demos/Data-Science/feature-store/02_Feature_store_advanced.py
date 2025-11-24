# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Feature store Travel Agency recommendation - Advanced
# MAGIC
# MAGIC In this advanced demo, we’ll explore more powerful capabilities of Feature Engineering in Databricks Unity Catalog, building on top of the previous introductory notebook.
# MAGIC
# MAGIC We’ll continue with the same use case — a Travel Agency Recommender Model — which aims to increase revenue by personalizing travel and hotel offers based on the likelihood that each user will make a purchase.
# MAGIC
# MAGIC This version goes deeper into temporal and online feature management, demonstrating how Databricks enables both accurate offline training and real-time serving.
# MAGIC
# MAGIC
# MAGIC ## What You’ll Learn
# MAGIC
# MAGIC - **Build advanced features across multiple Feature Tables**  
# MAGIC   Create user-level and destination-level features, including rolling aggregates and time-aware statistics.
# MAGIC
# MAGIC - **Introduce a timestamp key for point-in-time lookups**  
# MAGIC   Ensure that features used during training reflect only information available up to that point in time — eliminating data leakage.
# MAGIC
# MAGIC - **Combine multiple feature tables using `FeatureLookup`**  
# MAGIC   Automatically join features from different entities (e.g., users and destinations) during training.
# MAGIC
# MAGIC - **Train and register a model using the Feature Engineering Client**  
# MAGIC   Use feature-enriched data to train a custom model, log it with feature lineage, and register it in Unity Catalog.
# MAGIC
# MAGIC - **Publish Online Feature Tables for real-time inference/serving**  
# MAGIC   Sync your Delta Tables to Online Feature Stores powered by Databricks Lakebase — enabling low-latency access to features for serving endpoints or external applications.
# MAGIC
# MAGIC
# MAGIC  By the end of this notebook, you’ll understand how to move from batch feature engineering to a fully automated, real-time ML system — where your models and feature pipelines stay consistent, governed, and production-ready within Unity Catalog.
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=02_Feature_store_advanced&demo_name=feature-store&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Update feature engineering version
# MAGIC %pip install mlflow==2.22.0 databricks-feature-engineering==0.13.0 databricks-sdk>=0.62.0 databricks-automl-runtime==0.2.21 holidays==0.71 category-encoders==2.8.1 lightgbm==4.6.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init-basic-new 

# COMMAND ----------

# DBTITLE 1,Review our silver data
# MAGIC %sql SELECT * FROM travel_purchase limit 10

# COMMAND ----------

# MAGIC %sql select * from user_demography limit 10

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1: Create our Feature Tables
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/2025Q4_02_image1.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC In this second example, we'll introduce more tables and new features calculated with window functions.
# MAGIC
# MAGIC To simplify updates & refresh, we'll split them in 2 tables:
# MAGIC
# MAGIC * **User features**: contains all the features for a given user in a given point in time (location, previous purchases if any, tenure days etc)
# MAGIC * **Destination features**: data on the travel destination for a given point in time (interest tracked by the number of clicks & impression)
# MAGIC
# MAGIC ### Point-in-time support for feature tables
# MAGIC
# MAGIC Databricks Feature Store supports use cases that require point-in-time correctness.
# MAGIC
# MAGIC The data used to train a model often has time dependencies built into it. In our case, because we are adding rolling-window features, our Feature Table will contain data on all the dataset timeframe. 
# MAGIC
# MAGIC When we build our model, we must consider only feature values up until the time of the observed target value. If you do not explicitly take into account the timestamp of each observation, you might inadvertently use feature values measured after the timestamp of the target value for training. This is called “data leakage” and can negatively affect the model’s performance.
# MAGIC
# MAGIC Time series feature tables include a timestamp key column that ensures that each row in the training dataset represents the latest known feature values as of the row’s timestamp. 
# MAGIC
# MAGIC In our case, this timestamp key will be the `ts` field, present in our 2 feature tables.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Calculating the features
# MAGIC
# MAGIC Let's calculate the aggregated features from the vacation purchase logs for destinations and users. 
# MAGIC
# MAGIC The user features capture the user profile information such as past purchased price. Because the booking data does not change very often, it can be computed once per day in batch.
# MAGIC
# MAGIC The destination features include popularity features such as impressions and clicks, as well as pricing features such as price at the time of booking.

# COMMAND ----------

# DBTITLE 1,User Features Functions
from pyspark.sql import functions as F, Window as W

def create_user_behavior_features(travel_purchase_df):
    """
    Compute user-level rolling window behavior features.
    """
    travel_purchase_df = travel_purchase_df.withColumn("ts_l", F.col("ts").cast("long"))

    user_behavior_df = (
        travel_purchase_df
        .withColumn(
            "lookedup_price_7d_rolling_sum",F.sum("price").over(W.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(-7 * 86400, 0)))
        .withColumn(
            "lookups_7d_rolling_sum",F.count("*").over(W.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(-7 * 86400, 0)))
        .withColumn(
            "mean_price_7d",F.when(F.col("lookups_7d_rolling_sum") > 0,
                   F.col("lookedup_price_7d_rolling_sum") / F.col("lookups_7d_rolling_sum")).otherwise(F.lit(None)))
        .withColumn("tickets_purchased", F.col("purchased").cast("int"))
        .withColumn(
            "last_6m_purchases",F.sum("tickets_purchased").over(W.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(-6 * 30 * 86400, 0)).cast("double"))
        .select("user_id", "ts", "mean_price_7d", "last_6m_purchases")
    )
    return user_behavior_df


# COMMAND ----------

# DBTITLE 1,User Features
def create_user_features(travel_purchase_df, user_demography_df):
    """
    Combine user-level rolling behavior features with demographic info.
    Adds tenure_days = datediff(ts, first_login_date).
    """
    # Compute behavioral features
    user_behavior_df = create_user_behavior_features(travel_purchase_df)

    # Join static demographics
    combined_df = (
        user_behavior_df
        .join(user_demography_df, on="user_id", how="left")
        # tenure_days = event time - first login date
        .withColumn(
            "tenure_days",
            F.datediff(F.to_date("ts"), F.col("first_login_date")).cast(LongType())
        )
        .select(
            "user_id",
            "ts",
            "mean_price_7d",
            "last_6m_purchases",
            "age",
            "gender",
            "income_bracket",
            "loyalty_tier",
            "billing_state",
            "billing_city",
            "tenure_days"
        )
    )

    return combined_df

# Example use
user_demography_df = spark.table(f"{catalog}.{db}.user_demography")
travel_purchase_df = spark.table("travel_purchase")

user_features_df = create_user_features(travel_purchase_df, user_demography_df)
display(user_features_df.limit(20))


# COMMAND ----------

# DBTITLE 1,Destination Features
def create_destination_features(travel_purchase_df):
    """
    Computes the destination_features feature group.
    """
    return (
        travel_purchase_df
          .withColumn("clicked", F.col("clicked").cast("int"))
          .withColumn("sum_clicks_7d", 
            F.sum("clicked").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
          )
          .withColumn("sum_impressions_7d", 
            F.count("*").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
          )
          .select("destination_id", "ts", "sum_clicks_7d", "sum_impressions_7d")
    )  
destination_features_df = create_destination_features(spark.table('travel_purchase'))
display(destination_features_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating the Feature Table
# MAGIC
# MAGIC Let's use the FeatureStore client to save our 2 tables. Note the `timestamp_keys='ts'` parameters that we're adding during the table creation.
# MAGIC
# MAGIC Databricks Feature Store will use this information to automatically filter features and prevent from potential leakage.

# COMMAND ----------

# DBTITLE 1,user_features
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient(model_registry_uri="databricks-uc")
# help(fe.create_table)

# first create a table with User Features calculated above 
fe_table_name_users = f"{catalog}.{db}.user_features_advanced"
#fe.drop_table(name=fe_table_name_users)
fe.create_table(
    name=fe_table_name_users, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["user_id", "ts"],
    timestamp_keys="ts",
    df=user_features_df,
    description="User-level rolling behavior + demographic features with tenure tracking"
    # attach tags if necessary
    #,tags={"demo":"yes"}
)

# COMMAND ----------

# DBTITLE 1,destination_features
fe_table_name_destinations = f"{catalog}.{db}.destination_features_advanced"
# second create another Feature Table from popular Destinations
# for the second table, we show how to create and write as two separate operations
# fe.drop_table(name=fe_table_name_destinations)
fe.create_table(
    name=fe_table_name_destinations, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["destination_id", "ts"],
    timestamp_keys="ts", 
    schema=destination_features_df.schema,
    description="Destination Popularity Features"
    # attach tags if necessary
    #,tags={"demo":"yes"}
)
fe.write_table(name=fe_table_name_destinations, df=destination_features_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/2025Q4_02_image6.png?raw=true" style="float: right; margin-left: 10px" width="550px">
# MAGIC
# MAGIC As in our previous example, the 2 feature store tables were created and are available within Unity Catalog. 
# MAGIC
# MAGIC You can explore Catalog Explorer. You'll find all the features created, including a reference to this notebook and the version used during the feature table creation.
# MAGIC
# MAGIC Note that the id ts are automatically defined as PK and TS PK columns.
# MAGIC
# MAGIC Now that our features are ready, we can start creating the training dataset and train your customized models!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2: Train a model with FS and timestamp lookup
# MAGIC
# MAGIC The next step is to build a training dataset. 
# MAGIC
# MAGIC Because we have 2 feature tables, we'll add 2 `FeatureLookup` entries, specifying the key so that the feature store engine can join using this field.
# MAGIC
# MAGIC We will also add the `timestamp_lookup_key` property to `ts` so that the engine filter the features based on this key.

# COMMAND ----------

# DBTITLE 1,Split the time series data
ground_truth_df = spark.table('travel_purchase').select('user_id', 'destination_id', 'purchased', 'ts').sample(fraction=0.3, seed=42)

# Split based on time to define a training and inference set (we'll do train+eval on the past & test in the most current value)
training_labels_df = ground_truth_df.where("ts < '2022-11-01'")
test_labels_df = ground_truth_df.where("ts >= '2022-11-01'")

# COMMAND ----------

# DBTITLE 1,Create the feature lookup with the lookup keys
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.feature_store import feature_table, FeatureLookup

fe = FeatureEngineeringClient()

model_feature_lookups = [
      FeatureLookup(
          table_name=fe_table_name_destinations,
          lookup_key="destination_id",
          timestamp_lookup_key="ts"
      ),
      FeatureLookup(
          table_name=fe_table_name_users,
          lookup_key="user_id",
          feature_names=["mean_price_7d", "last_6m_purchases", "tenure_days", "age", "gender", "income_bracket", "loyalty_tier", "billing_state"], 
          timestamp_lookup_key="ts"
      )
]

# fe.create_training_set will look up features in model_feature_lookups with matched key from training_labels_df
training_set = fe.create_training_set(
    df=training_labels_df, # joining the original Dataset, with our FeatureLookupTable
    feature_lookups=model_feature_lookups,
    exclude_columns=["ts", "destination_id", "user_id"], # exclude id columns as we don't want them as feature
    label='purchased',
)

test_set = fe.create_training_set(
    df=test_labels_df,
    feature_lookups=model_feature_lookups,
    exclude_columns=["ts", "destination_id", "user_id"],
    label="purchased",  # keep label here for offline evaluation
)


training_pd = training_set.load_df().toPandas()
test_pd = test_set.load_df().toPandas()
X_train, y_train = training_pd.drop("purchased", axis=1), training_pd["purchased"]
X_test,  y_test  = test_pd.drop("purchased", axis=1),  test_pd["purchased"]

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Training the Model with Custom scikit-learn Pipelines
# MAGIC
# MAGIC In this session we’ll manually train a **custom scikit-learn classification model** using the features retrieved from the Feature Store.
# MAGIC
# MAGIC We’ll build a preprocessing and modeling pipeline that includes:
# MAGIC - A `ColumnTransformer` to handle both numerical and categorical features
# MAGIC - Standardization and one-hot encoding for consistent feature scaling
# MAGIC - A set of classification models include `LightGBMClassifier`, `Random Forest` as our core algorithms for predicting travel purchases
# MAGIC
# MAGIC Throughout the process, we’ll use **MLflow tracking** and the **Feature Engineering Client** to:
# MAGIC - Train multiple models with model selection on the best model
# MAGIC - Log our best performed model, parameters, and metrics  
# MAGIC - Capture full feature lineage from the Feature Store  
# MAGIC - Register the trained model in **Unity Catalog** for governance and deployment
# MAGIC
# MAGIC This approach provides full control over feature engineering, preprocessing, and model selection, and maintaining the same reproducibility and lineage benefits.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Experiment Setup
xp_path = "/Shared/dbdemos/experiments/feature-store"
xp_name = f"time_based_fs_model_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"


print(f"Starting experiment: {xp_name}")

numerical_features = X_train.select_dtypes(include=["int64", "float64"]).columns.tolist()
categorical_features = X_train.select_dtypes(include=["object"]).columns.tolist()

print(f"Numerical features: {len(numerical_features)}, Categorical: {len(categorical_features)}")

# Shared preprocessing
numeric_transformer = Pipeline([("scaler", StandardScaler())])
categorical_transformer = Pipeline(
    [("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False))]
)
preprocessor = ColumnTransformer([
    ("num", numeric_transformer, numerical_features),
    ("cat", categorical_transformer, categorical_features)
])


# COMMAND ----------

# DBTITLE 1,Candidate models
best_model = None
best_auc = -1
best_name = None

# Candidate models
candidates = {
    "log_reg": LogisticRegression(max_iter=500),
    "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
    "gbm": GradientBoostingClassifier(random_state=42),
    "lightgbm": LGBMClassifier(random_state=42)
}

# COMMAND ----------

# DBTITLE 1,Train & Eval
mlflow.set_experiment(xp_path)
x_sample = X_train.sample(1, random_state=42)
y_sample = y_train.sample(1, random_state=42)
signature = infer_signature(X_train, y_sample)

for name, clf in candidates.items():
    with mlflow.start_run(run_name=name):
        model = Pipeline([
            ("preprocessor", preprocessor),
            ("classifier", clf)
        ])
        model.fit(X_train, y_train)

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_pred_proba)
        acc = accuracy_score(y_test, (y_pred_proba > 0.5).astype(int))

        print(f"{name:15s} | AUC: {auc:.4f} | ACC: {acc:.4f}")

        #  Log parameters and metrics
        mlflow.log_param("model_name", name)
        mlflow.log_metrics({"auc": auc, "accuracy": acc})

        #  Log model for each candidate
        mlflow.sklearn.log_model(model, artifact_path="model", signature=signature)

        if auc > best_auc:
            best_auc, best_model, best_name = auc, model, name
            mlflow.set_tag("best_model", "true")

print(f"\nBest model: {best_name} (AUC={best_auc:.4f})")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Saving our best model to MLflow registry
# MAGIC
# MAGIC Next, we'll log the best model as a new run using the `FeatureStoreClient.log_model()` function. 

# COMMAND ----------

# DBTITLE 1,Log best model with FS lineage
model_name = "dbdemos_fs_travel_model_advanced"
model_full_name = f"{catalog}.{db}.{model_name}"

mlflow.set_registry_uri("databricks-uc")

# Conda environment cleanup
env = mlflow.pyfunc.get_default_conda_env()
for dep in env["dependencies"]:
    if isinstance(dep, dict) and "pip" in dep:
        dep["pip"] = [pkg for pkg in dep["pip"] if not pkg.startswith("mlflow==")]
        dep["pip"].insert(0, f"mlflow=={mlflow.__version__}")
        dep["pip"].insert(1, "numpy<2.0")

x_sample = X_train.sample(1, random_state=42)
signature = infer_signature(X_train, best_model.predict(X_train))

#All in ONE MLflow run — preserves feature lineage
with mlflow.start_run(run_name=f"{xp_name}_{best_name}") as run:
    mlflow.log_metric("test_auc", best_auc)
    mlflow.log_param("best_model_type", best_name)
    mlflow.log_input(mlflow.data.from_pandas(X_train.sample(10, random_state=42)), "training_sample")

    fe.log_model(
        model=best_model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,     # FS lineage preserved here       
        input_example=x_sample,
        signature=signature,
        registered_model_name=model_full_name,
        conda_env=env
    )

print(f"\n Model logged and registered with FS metadata: {model_full_name}")
print("This model will now support automatic feature lookup in Model Serving.")

# COMMAND ----------

# DBTITLE 1,Promote the best model into Production
latest_model = get_last_model_version(model_full_name)
#Move it in Production
production_alias = "production"
if len(latest_model.aliases) == 0 or latest_model.aliases[0] != production_alias:
  print(f"updating model {latest_model.version} to Production")
  mlflow_client = MlflowClient(registry_uri="databricks-uc")
  mlflow_client.set_registered_model_alias(model_full_name, production_alias, version=latest_model.version)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3: Running batch inference
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/2025Q4_02_image2.png" style="float: right" width="850px" />
# MAGIC
# MAGIC As we saw previously, we can easily leverage the feature store to get our predictions.
# MAGIC
# MAGIC No need to fetch or recompute the feature, we just need the lookup ids and the feature store will automatically fetch them from the feature store table. 

# COMMAND ----------

# DBTITLE 1,Batch inference
## For sake of simplicity, we will just predict on the same inference_data_df
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient(model_registry_uri="databricks-uc")
batch_scoring = test_labels_df.select('user_id', 'destination_id', 'ts', 'purchased')
scored_df = fe.score_batch(model_uri=f"models:/{model_full_name}@{production_alias}", df=batch_scoring, result_type="boolean")
display(scored_df.limit(100))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 4: Real-Time Inference with Databricks Online Feature Stores
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/2025Q4_02_image4.png?raw=true" style="float: right" width="850px" />
# MAGIC
# MAGIC
# MAGIC Databricks now supports **Online Feature Stores**, providing a fully managed and low-latency key–value lookup service for real-time machine learning applications.
# MAGIC
# MAGIC In this advanced demo, we publish our offline feature tables (for example, `user_features_advanced` and `destination_features_advanced`) to an **online feature store** using the `FeatureEngineeringClient.publish_table()` API.  
# MAGIC Once published, these tables are continuously synchronized with their Delta source tables, ensuring that your real-time applications always access the latest feature values.
# MAGIC
# MAGIC With the online tables in place, we can:
# MAGIC - Create a **Feature Spec** to define which features should be fetched for inference  
# MAGIC - Build a **Feature Serving Endpoint** that exposes these features through an API  
# MAGIC - Connect the endpoint to our **Unity Catalog–registered ML model**, allowing it to automatically fetch the latest features for each incoming request
# MAGIC
# MAGIC This architecture enables **end-to-end real-time prediction**, where feature retrieval and model inference happen within milliseconds — all governed and tracked within Unity Catalog.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Creating the online feature store
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/2025Q4_02_image7.png?raw=true" style="float: right" width="700px" />
# MAGIC
# MAGIC The capacity options correspond to different performance tiers "CU_1", "CU_2", "CU_4", and "CU_8". Each capacity unit allocates about 16GB of RAM to the database instance, along with all associated CPU and local SSD resources.
# MAGIC
# MAGIC Once you created the online store, it should be avaiable under Compute -> Lakebase Postgres
# MAGIC
# MAGIC

# COMMAND ----------

fe = FeatureEngineeringClient()
# Try to create online store with error handling
store_name = "dbdemo-travel-online-store"

try:
    # First check if it already exists
    store = fe.get_online_store(name=store_name)
    print(f"Online store already exists: {store.name}, State: {store.state}, Capacity: {store.capacity}")
except:
    # Store doesn't exist, try to create it
    try:
        print(f"Creating online store '{store_name}'...")
        fe.create_online_store(
            name=store_name,
            capacity="CU_1"  # Using smaller capacity to reduce timeout risk
        )
        print(f"Online store '{store_name}' creation initiated. This may take several minutes.")
        
        # Wait a bit and check status
        time.sleep(500)
        store = fe.get_online_store(name=store_name)
        print(f"Store: {store.name}, State: {store.state}, Capacity: {store.capacity}")
        
    except Exception as e:
        print(f"Error creating online store: {e}")
        print("Online store creation can take time. You may need to wait and check status later.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish a feature table to an online store
# MAGIC
# MAGIC #### Prerequisites for publishing to online stores
# MAGIC All feature tables (with or without time series) must meet these requirements before publishing:
# MAGIC
# MAGIC - **Primary key constraint**: Required for online store publishing
# MAGIC - **Non-nullable primary keys**: Primary key columns cannot contain NULL values
# MAGIC - **Change Data Feed enabled**: Required for online store sync. See [Enable change data feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed#enable)
# MAGIC
# MAGIC
# MAGIC After your online store is in the AVAILABLE state, you can publish feature tables to make them available for low-latency access.

# COMMAND ----------

# DBTITLE 1,Prerequisites Check- Change the catalog/schema
# MAGIC %sql
# MAGIC -- User feature table
# MAGIC ALTER TABLE user_features_advanced
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC ALTER TABLE user_features_advanced
# MAGIC ALTER COLUMN user_id SET NOT NULL;
# MAGIC
# MAGIC -- Destination feature table
# MAGIC ALTER TABLE destination_features_advanced
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC ALTER TABLE destination_features_advanced
# MAGIC ALTER COLUMN destination_id SET NOT NULL;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Publish feature tables
from databricks.ml_features.entities.online_store import DatabricksOnlineStore
# Retrieve the store object
online_store = fe.get_online_store(name="dbdemo-travel-online-store")

# Publish user-level features
fe.publish_table(
    online_store=online_store,
    source_table_name=f"{catalog}.{db}.user_features_advanced",
    online_table_name=f"{catalog}.{db}.user_features_advanced_online",
    publish_mode="CONTINUOUS"
)

# Publish destination-level features
fe.publish_table(
    online_store=online_store,
    source_table_name=f"{catalog}.{db}.destination_features_advanced",
    online_table_name=f"{catalog}.{db}.destination_features_advanced_online",
    publish_mode="CONTINUOUS"
)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Our online feature tables are available in the Unity Catalog Explorer, like any other tables!
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/2025Q4_02_image3.png?raw=true" style="float: right" width="650px" />
# MAGIC
# MAGIC We can see that our online table has been successfully created. 
# MAGIC
# MAGIC Like any other table, it's available within the Unity Catalog explorer, in your catalog -> schema. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Waiting for Lakebase table
# Wait for both tables to be ready before continuing
tables_ready = wait_for_lakebase_tables(
    catalog=catalog,
    schema=db,
    tables=[
        "user_features_advanced_online",
        "destination_features_advanced_online"
    ],
    waiting_time=1200,  # 30 minutes max wait
    sleep_time=30
)

if not tables_ready:
    raise RuntimeError("Not all online tables reached ONLINE state. Please check the synchronization status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Real-time Serving Scenarios
# MAGIC
# MAGIC There are two main ways to operationalize your features and models:
# MAGIC
# MAGIC ### Option 1. Feature Serving Endpoint (for external applications)
# MAGIC
# MAGIC Use when your application or model runs outside of Databricks, but still needs low-latency access to features.
# MAGIC Databricks will automatically keep the online feature tables in sync, and your application can fetch the latest features via a REST API.
# MAGIC
# MAGIC ### Option 2. Model Serving Endpoint (for in-Databricks inference)
# MAGIC
# MAGIC Use when your model is deployed in Databricks Model Serving.
# MAGIC The model automatically performs online feature lookups from the published tables — no extra feature-joining logic required.
# MAGIC This enables true real-time inference with consistent feature definitions across training, batch scoring, and serving.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/2025Q4_02_image5.png?raw=true" style="float: right" width="750px" />

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Option 1. Feature Serving Endpoint (for external applications)
# MAGIC
# MAGIC #### Option 1 - Step 1: Create a FeatureSpec
# MAGIC A FeatureSpec is a user-defined set of features and functions. You can combine features and functions in a FeatureSpec. FeatureSpecs are stored in and managed by Unity Catalog and appear in Catalog Explorer.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Option 1 - Create a FeatureSpec
# Create a Feature Spec that combines both user & destination online features
feature_spec_name = f"{catalog}.{db}.travel_purchase_feature_spec"

# Create the feature spec using FeatureLookup definitions
fe.create_feature_spec(
    name=feature_spec_name,
    features=[
        FeatureLookup(
            table_name=f"{catalog}.{db}.user_features_advanced",
            lookup_key="user_id"
        ),
        FeatureLookup(
            table_name=f"{catalog}.{db}.destination_features_advanced",
            lookup_key="destination_id"
        ),
    ],
)

print(f"Feature Spec created: {feature_spec_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 1 - Step 2: Create a Feature serving endpoint
# MAGIC
# MAGIC The FeatureSpec defines the endpoint, once the feature serving endpoint is created, click Serving in the left sidebar of the Databricks UI. When the state is Ready, the endpoint is ready to respond to queries.

# COMMAND ----------

# DBTITLE 1,Option 1 - Create a Feature serving endpoint
from databricks.feature_engineering.entities.feature_serving_endpoint import (
    ServedEntity,
    EndpointCoreConfig,
)

fs_endpoint_name = f"{catalog}_{schema}_travel-recommendation-fs"[:50]

endpoint_config = EndpointCoreConfig(
    served_entities=ServedEntity(
        feature_spec_name=feature_spec_name,
        workload_size="Small",          # Small / Medium / Large
        scale_to_zero_enabled=True,     # Save cost when idle
    )
)

# Create the Feature Serving Endpoint
fe.create_feature_serving_endpoint(
    name=fs_endpoint_name,
    config=endpoint_config
)

print(f"Feature Serving Endpoint created: {fs_endpoint_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 1 - Step 3: Query the feature serving endpoint
# MAGIC You can use the REST API, the MLflow Deployments SDK, or the Serving UI to query an endpoint in order to get real-time access to the calculated features.

# COMMAND ----------

# DBTITLE 1,Ensure the endpoints is READY
if wait_until_endpoint_ready(fs_endpoint_name, timeout=900, sleep_time=30):
    print("Endpoint confirmed ready — safe to send inference or feature requests.")
else:
    raise TimeoutError(f"Endpoint '{fs_endpoint_name}' is not ready yet.")


# COMMAND ----------

# DBTITLE 1,Option 1 - Real time feature access
import mlflow.deployments

# Initialize MLflow deployment client for Databricks
client = mlflow.deployments.get_deploy_client("databricks")

# Get your actual endpoint name from the previous step
#fs_endpoint_name = f"travel-recommendation-fs-{int(time.time())}"

# Prepare sample inputs (these correspond to your lookup keys)
# You can query multiple user_id + destination_id pairs
response = client.predict(
    endpoint=fs_endpoint_name,
    inputs={
        "dataframe_records": [
            {"user_id": 1001, "destination_id": 42},
            {"user_id": 1055, "destination_id": 91},
            {"user_id": 1234, "destination_id": 12},
        ]
    },
)

print("Feature Serving Endpoint Response:")
print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2. Model Serving Endpoint (for in-Databricks inference)
# MAGIC
# MAGIC When you use Mosaic AI Model Serving to serve a model that was built using features from Databricks, the model automatically looks up and transforms features for inference requests. 
# MAGIC
# MAGIC #### Option 2 - Step 1: Create a model serving endpoint
# MAGIC The following variables set the values for configuring the model serving endpoint, such as the endpoint name, compute type, and which model to serve with the endpoint. After you call the create endpoint API, the logged model is deployed to the endpoint.

# COMMAND ----------

# DBTITLE 1,Option 2 - Create a model serving endpoint
# Get latest version dynamically from the registry
client = mlflow.deployments.get_deploy_client("databricks")
latest_version = latest_model.version
ms_endpoint_name = f"{catalog}_{schema}_travel-recommendation-ms"[:50]

endpoint_config = {
    "served_entities": [
        {
            "entity_name": model_full_name,       # full UC model path
            "entity_version": str(latest_version),
            "workload_size": "Small",             # Small / Medium / Large
            "scale_to_zero_enabled": True
        }
    ],
    "traffic_config": {
        "routes": [
            {
                "served_model_name": f"{model_name}-{latest_version}",
                "traffic_percentage": 100
            }
        ]
    }
}
# For first time creation
endpoint = client.create_endpoint(name=ms_endpoint_name,config=endpoint_config)
# For updating the endpoint
#endpoint = client.update_endpoint(ms_endpoint_name, endpoint_config)

print(f"Endpoint '{ms_endpoint_name}' created or updated successfully.")
print(f"Model served: {model_full_name} (version {latest_version})")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### View your endpoint
# MAGIC For more information about your endpoint, go to the Serving UI and search for your endpoint name.

# COMMAND ----------

# DBTITLE 1,Ensure your endpoint is READY
if wait_until_endpoint_ready(ms_endpoint_name, timeout=900, sleep_time=30):
    print("Endpoint confirmed ready — safe to send inference or feature requests.")
else:
    raise TimeoutError(f"Endpoint '{ms_ndpoint_name}' is not ready yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 2 - Step 2: Query your endpoint
# MAGIC
# MAGIC Once your endpoint is ready, you can query it by making an API request to run predictions. Depending on the model size and complexity, it can take 30 minutes or more for the endpoint to get ready.

# COMMAND ----------

# DBTITLE 1,Option 2 - Real-time prediction
import mlflow.deployments

# Initialize MLflow deployment client for Databricks
client = mlflow.deployments.get_deploy_client("databricks")

# Get your actual model serving endpoint name from the previous step
#ms_endpoint_name = f"travel-recommendation-ms-{int(time.time())}"

# Prepare sample inputs (these correspond to your lookup keys)
# You can query multiple user_id + destination_id pairs
response = client.predict(
    endpoint=ms_endpoint_name,
    inputs={
        "dataframe_records": [
            {"user_id": 1234, "destination_id": 12},
        ]
    },
)

print("Feature Serving Endpoint Response:")
print(response)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary 
# MAGIC
# MAGIC In this advanced demo, we’ve built a complete feature lifecycle using Databricks Feature Store — from creating timestamp-aware user and destination features to training and deploying a real-time recommendation model.
# MAGIC
# MAGIC With Unity Catalog integration, every feature and model is fully governed and traceable. You can now see exactly which model depends on which feature table, and trace it back to the notebook or job that created it.
# MAGIC
# MAGIC By registering the model through `fe.log_model()`, we’ve ensured automatic feature lookups during inference — whether the model runs in batch jobs or is served in real time through Databricks Model Serving.
# MAGIC This guarantees that the same features used in training are consistently retrieved for inference, eliminating data leakage and mismatches.
# MAGIC
# MAGIC Next Step: Spark Declarative Pipeline Feature Creation ([open the notebook]($./03_Feature_store_pipeline))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Declarative Pipeline Feature Creation 
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/2025Q4_03_image1.png?raw=true" style="float: right" width="500px" />
# MAGIC
# MAGIC Next, we’ll demonstrate how to build and manage feature tables declaratively using Lakeflow pipelines.
# MAGIC With a simple Python decorator like `@dp.materialized_view`, Databricks can automatically orchestrate feature dependencies, manage schema evolution, and handle refresh schedules — unifying feature engineering and pipeline orchestration within a single framework.
# MAGIC
