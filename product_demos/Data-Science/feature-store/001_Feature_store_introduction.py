# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Getting started with Feature Engineering in Databricks Unity Catalog
# MAGIC
# MAGIC The <a href="https://docs.databricks.com/en/machine-learning/feature-store/uc/feature-tables-uc.html" target="_blank">Feature Engineering in Databricks Unity Catalog</a> allows you to create a centralized repository of features. These features can be used to train & call your ML models. By saving features as feature engineering tables in Unity Catalog, you will be able to:
# MAGIC
# MAGIC - Share features across your organization 
# MAGIC - Increase discoverability sharing 
# MAGIC - Ensures that the same feature computation code is used for model training and inference
# MAGIC - Enable real-time backend, leveraging your Delta Lake tables for batch training and Key-Value store for realtime inferences
# MAGIC
# MAGIC ## Demo content
# MAGIC
# MAGIC Multiple version of this demo are available, each version introducing a new concept and capabilities. We recommend following them one by one.
# MAGIC
# MAGIC ### Introduction (this notebook)
# MAGIC
# MAGIC - Ingest and prepare raw data.
# MAGIC - Create and register Feature Tables in Unity Catalog.
# MAGIC - Use `FeatureLookup` to join multiple feature sources.
# MAGIC - Train a model using the **Feature Engineering Client** (`FeatureEngineeringClient`).
# MAGIC - Register and promote the model to production in **Unity Catalog**.
# MAGIC - Perform **batch inference** using feature lineage.
# MAGIC
# MAGIC ### Advanced version ([open the notebook]($./002_Feature_store_advanced))
# MAGIC
# MAGIC - Combine multiple Feature Tables using **point-in-time lookup**.
# MAGIC - Create **Online Feature Tables** for real-time serving.
# MAGIC - Build and publish a **Feature Spec** for online inference.
# MAGIC - Deploy and query **Feature Serving Endpoints**.
# MAGIC - Serve a **UC-registered model** in real time with automatic feature lookup.
# MAGIC
# MAGIC ### Spark Declarative Pipeline Feature Creation ([open the notebook]($./003_Feature_store_pipeline))
# MAGIC
# MAGIC - Demonstrate how to build and manage feature tables declaratively using Lakeflow pipelines.
# MAGIC
# MAGIC With a simple Python decorator like `@dp.materialized_view`, Databricks can automatically orchestrate feature dependencies, manage schema evolution, and handle refresh schedules — unifying feature engineering and pipeline orchestration within a single framework.
# MAGIC
# MAGIC  
# MAGIC *For more detail on the Feature Engineering in Unity Catalog, open <a href="https://api-docs.databricks.com/python/feature-engineering/latest" target="_blank">the documentation</a>.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01_Feature_store_introduction&demo_name=feature-store&event=VIEW">

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##Building a Travel Purchase Propensity Model
# MAGIC
# MAGIC In this demo, we’ll take on the role of a **travel booking platform** looking to increase revenue by personalizing travel and hotel offers.
# MAGIC
# MAGIC Our goal as data scientists is to build a simple **Travel Purchase Propensity Model** — a model that predicts the likelihood that a user will purchase a specific travel package.  
# MAGIC This will serve as the foundation for personalized recommendations and targeted marketing campaigns.
# MAGIC
# MAGIC For this **introductory version**, we’ll keep things simple and focus on the fundamentals of Feature Engineering in Unity Catalog:
# MAGIC
# MAGIC - Start from the **travel purchase logs**, which capture user interactions such as clicks, prices, and bookings.
# MAGIC - Create a **Feature Table** (`destination_location_fs`) that aggregates travel-related behaviors (e.g., total destination clicks, average price, conversion rate).
# MAGIC - Generate a second **Feature Table** (`user_demography`) that includes static user attributes such as age, income bracket, loyalty tier, and billing location.
# MAGIC - Use the **Feature Engineering Client** to join these features, train a baseline model, and log it into Unity Catalog with feature lineage.
# MAGIC - Finally, perform **batch inference** using the same features to predict which users are most likely to make a purchase.
# MAGIC
# MAGIC
# MAGIC > 💡 The goal of this notebook is to introduce the core concepts of Feature Tables and how they integrate with the model lifecycle in Databricks — not to optimize model accuracy.
# MAGIC

# COMMAND ----------

# MAGIC %pip install mlflow==2.22.0 databricks-feature-engineering==0.10.2 databricks-sdk==0.50.0 databricks-automl-runtime==0.2.21 holidays==0.71 category-encoders==2.8.1 lightgbm==4.6.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init-basic-new $reset_all_data=true

# COMMAND ----------

# DBTITLE 1,Let's review our silver table we'll use to create our features
# MAGIC %sql 
# MAGIC SELECT * FROM travel_purchase

# COMMAND ----------

# DBTITLE 1,Let's also review the user demography table
# MAGIC %sql
# MAGIC select * from user_demography

# COMMAND ----------

# MAGIC %md 
# MAGIC Note that a Data Sciencist would typically start by exploring the data. We could also use the data profiler integrated into Databricks Notebooks to quickly identify if we have missings values or a skew in our data.
# MAGIC
# MAGIC *We will keep this part simple as we'll focus on feature engineering*

# COMMAND ----------

# DBTITLE 1,Quick data analysis
import seaborn as sns
g = sns.PairGrid(spark.table('travel_purchase').sample(0.01).toPandas()[['price', 'user_latitude', 'user_longitude', 'purchased']], diag_sharey=False, hue="purchased")
g.map_lower(sns.kdeplot).map_diag(sns.kdeplot, lw=3).map_upper(sns.regplot).add_legend()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1: Create our Feature Engineering table
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/2025Q4_01_image1.png" alt="Feature Engineering Table Creation" width="500px" style="margin-left: 10px; float: right"/>
# MAGIC
# MAGIC ## Creating the First Feature Table
# MAGIC
# MAGIC Our first step is to create a **Feature Table** in Unity Catalog.
# MAGIC
# MAGIC We’ll start by loading data from the **silver table** `travel_purchase`, which contains user interactions such as clicks, prices, and purchases.  
# MAGIC From these values, we’ll engineer new features that can be used by our model.
# MAGIC
# MAGIC In this introductory version, we’ll focus on a few simple transformations:
# MAGIC - Deriving aggregated destination-level features such as total clicks, purchases, and conversion rates.
# MAGIC - Dropping the label column (`purchased`) to prevent feature leakage during model training.
# MAGIC
# MAGIC To create the feature table, we’ll use the `FeatureEngineeringClient.create_table()` API.  
# MAGIC Under the hood, this writes the feature data as a **Delta Table** in Unity Catalog and registers it as a managed feature table for reuse.
# MAGIC
# MAGIC 💡 In a production environment, this logic would typically live in a **scheduled feature pipeline or Lakeflow job** that refreshes features automatically whenever new data arrives in the silver table.
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Compute the features 
# MAGIC
# MAGIC Let's create the features that we'll save in our Feature Table using simple SQL query.
# MAGIC
# MAGIC This transformation would typically be part of a job used to refresh our feature, triggered for model training and inference so that the features are computed with the same code.

# COMMAND ----------

# DBTITLE 1,Create the aggregated features with SQL
# MAGIC %sql
# MAGIC -- Destination-level features
# MAGIC CREATE OR REPLACE TEMP VIEW destination_features AS
# MAGIC SELECT
# MAGIC   destination_id,
# MAGIC   COUNT(*) AS total_dest_interactions,
# MAGIC   SUM(CAST(clicked AS INT)) AS total_dest_clicks,
# MAGIC   SUM(CAST(purchased AS INT)) AS total_dest_purchases,
# MAGIC   AVG(price) AS avg_dest_price,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CAST(clicked AS INT)) > 0 THEN SUM(CAST(purchased AS INT)) / SUM(CAST(clicked AS INT))
# MAGIC     ELSE 0
# MAGIC   END AS dest_ctr
# MAGIC FROM travel_purchase
# MAGIC GROUP BY destination_id;

# COMMAND ----------

df = spark.table("destination_features")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the Feature Engineering Table
# MAGIC
# MAGIC Next, we will save our feature as a Feature Engineering Table using the **`create_table`** method.
# MAGIC
# MAGIC We'll need to give it a name and a primary key that we'll use for lookup. Primary key should be unique. In this case we'll use the booking id.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's start creating a <a href="https://docs.databricks.com/en/machine-learning/feature-store/uc/feature-tables-uc.html#create-a-feature-table-in-unity-catalog&language-Python" target="_blank">Feature Engineering Client</a>. Calling `create_table` on this client will result in a table being created in Unity Catalog. 

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient(model_registry_uri="databricks-uc")

fe.create_table(
    name="destination_location_fs",
    #name="summer_catalog.summer_schema.travel_purchase_features",
    primary_keys=["destination_id"],
    df=df,
    description="Travel purchases dataset with feature engineering",
    tags={"team":"data_science"}
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Alternatively, you can first **`create_table`** with a schema only, and populate data to the feature table with **`fs.write_table`**. To add data you can simply use **`fs.write_table`** again. **`fs.write_table`** supports a **`merge`** mode to update features based on the primary key. To overwrite a feature table you can simply `DELETE` the existing records directly from the feature table before writing new data to it, again with **`fs.write_table`**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Feature Table from an Existing Unity Catalog Table
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/2025Q4_01_image2.png" alt="Feature Engineering Table Creation" width="500px" style="margin-left: 10px; float: right"/>
# MAGIC
# MAGIC
# MAGIC In some cases, your feature data may already exist as a Delta table in Unity Catalog.
# MAGIC
# MAGIC For example, our **user demographic data** (age, gender, income, loyalty tier, billing location, etc.) is stored in an existing table.
# MAGIC
# MAGIC
# MAGIC To use this table as a **Feature Table**, you simply need to define a **primary key constraint**.  
# MAGIC
# MAGIC Once the primary key is set, the table automatically appears in the **Features** tab within Unity Catalog and becomes available for retrieval using `FeatureLookup`.
# MAGIC
# MAGIC
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 You do **not** need to call `fe.create_table()` again — that step is only required when creating a new feature table directly from a DataFrame.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Make sure user_id is not nullable
# MAGIC ALTER TABLE user_demography ALTER COLUMN user_id SET NOT NULL;
# MAGIC
# MAGIC -- Add primary key constraint
# MAGIC ALTER TABLE user_demography ADD CONSTRAINT user_demography_pk PRIMARY KEY(user_id);

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Our Feature Table is now available in Unity Catalog
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-basic-fs-table.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC You can explore it directly from the **Unity Catalog Explorer**:
# MAGIC 1. Select your **catalog** and **schema**.  
# MAGIC 2. Browse the list of **Feature Tables** to find the one you just created (`user_demography` or `destination_location_fs`).  
# MAGIC 3. Click the table name to view its schema, sample data, and metadata.
# MAGIC
# MAGIC In addition to using the UI, you can also inspect the table programmatically with:
# MAGIC
# MAGIC ```python
# MAGIC fe.get_table(name="destination_location_fs")
# MAGIC

# COMMAND ----------

fe_get_table = fe.get_table(name="destination_location_fs")
print(f"Feature Table in UC=destination_location_fs. Description: {fe_get_table.description}")
print("The table contains those features: ", fe_get_table.features)

# COMMAND ----------

fe_get_user = fe.get_table(name="user_demography")
print(f"Feature Table in UC=user_demography. Description: {fe_get_user.description}")
print("The table contains those features: ", fe_get_user.features)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2: Train a model with FS 
# MAGIC Now that our Feature Tables are ready, we can train a machine learning model using the features stored in Unity Catalog.
# MAGIC
# MAGIC The first step is to build our **training dataset**.  
# MAGIC We’ll start with a list of entity identifiers (in this case, `destination_id` and `user_id`) and their corresponding label (`purchased`) — the outcome we want to predict.
# MAGIC
# MAGIC Using a `FeatureLookup` list, we’ll automatically retrieve all related features from our registered Feature Tables.  
# MAGIC The lookup process joins the raw label dataset with the appropriate feature tables based on the defined **lookup keys**.
# MAGIC
# MAGIC Once our feature-enriched training dataset is ready, we’ll:
# MAGIC 1. Train a baseline model using these features.  
# MAGIC 2. Log and register the model in Unity Catalog with full **feature lineage** tracking.  
# MAGIC 3. Deploy the model to **production** for batch inference.
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/2025Q4_01_image3.png" style="margin-left: 10px" width="1200px">
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Build the training dataset 
# MAGIC
# MAGIC Let's start by building the dataset, retrieving features from our feature table.

# COMMAND ----------

# DBTITLE 1,Get our list of id & labels
id_and_label = spark.table('travel_purchase').select("id", "purchased", "destination_id", "user_id")
display(id_and_label)

# COMMAND ----------

# DBTITLE 1,Retrieve the features from the feature table
model_feature_lookups = [
    FeatureLookup(
        table_name="destination_location_fs",
        lookup_key="destination_id",   # join on destination_id (destination-level features)
        # feature_names=["avg_dest_price", "dest_ctr"],  # optionally specify if you want fewer columns
    ),
    FeatureLookup(
        table_name="user_demography",
        lookup_key="user_id",          # join on user_id (user-level features)
        feature_names=["age", "gender", "income_bracket", "loyalty_tier"], 
    )
]
# fe.create_training_set will look up features in model_feature_lookups with matched key from training_labels_df
training_set = fe.create_training_set(
    df=id_and_label,                   # your base DataFrame containing id, user_id, destination_id, purchased
    feature_lookups=model_feature_lookups,
    exclude_columns=["id", "user_id", "destination_id", "booking_date"],  # exclude join keys and metadata
    label="purchased"
)

training_pd = training_set.load_df().toPandas()
display(training_pd)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Introducing Databricks Assistant Data Science Agent
# MAGIC We are proud to introduce the Data Science Agent, a major advancement that elevates the Databricks Assistant from a helpful copilot into a true autonomous partner for data science and analytics. Fully integrated with Databricks Notebooks and the SQL Editor, the [Data Science Agent](https://www.databricks.com/blog/introducing-databricks-assistant-data-science-agent) brings intelligence, adaptability, and execution together in a single experience.
# MAGIC <img src="https://www.databricks.com/sites/default/files/2025-09/AgentModeOG1Border.png?v=1756901406" width="500px" style="float: right"/>
# MAGIC - Data Science Agent transforms Databricks Assistant into an autonomous partner for data science and analytics tasks in Notebooks and the SQL Editor.
# MAGIC
# MAGIC - It can explore data, generate and run code, and fix errors, all from a single prompt. This can cut hours of work to minutes.
# MAGIC
# MAGIC - Purpose-built for common data science tasks and grounded in Unity Catalog for seamless, governed access to your data.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Training our baseline model 
# MAGIC
# MAGIC Note that for our first basic example, the feature used are very limited and our model will very likely not be efficient, but we won't focus on the model performance.
# MAGIC
# MAGIC The following steps will be a basic LGBM model. Note that to log the model, we'll use the `FeatureEngineeringClient.log_model(...)` function and not the usual `mlflow.skearn.log_model(...)`. This will capture all the feature dependencies & lineage for us and update the feature table data.

# COMMAND ----------

# DBTITLE 1,Split the dataset
# Split features/labels
training_pd = training_pd.sample(frac=0.2, random_state=42)
X = training_pd.drop("purchased", axis=1)
y = training_pd["purchased"].values.ravel()
x_train, x_val, y_train, y_val = train_test_split(X, y, test_size=0.1, stratify=y, random_state=42)

# Identify feature types
numerical_features = [col for col in X.columns if X[col].dtype in [np.int64, np.float64]]
categorical_features = [col for col in X.columns if X[col].dtype == object]

print(f"x_train: {x_train.shape}, y_train: {y_train.shape}")
print(f"x_val:   {x_val.shape}, y_val:   {y_val.shape}")

# COMMAND ----------

# DBTITLE 1,FS Configuration
# MLflow & FS config
mlflow.sklearn.autolog(log_input_examples=True,silent=True)
model_name = "dbdemos_fs_travel_model"
model_full_name = f"{catalog}.{db}.{model_name}"
dataset = mlflow.data.from_pandas(x_train)

# COMMAND ----------

# DBTITLE 1,Train a model
fe = FeatureEngineeringClient()

with mlflow.start_run(run_name="lightGBM") as run:
    # Log dataset metadata
    mlflow.log_input(dataset, context="training")

    # Define preprocessing pipelines
    num_pipeline = Pipeline([
      ("fillna", FunctionTransformer(lambda df: df.fillna(df.mean()))),("scale", StandardScaler())
    ])

    cat_pipeline = Pipeline([
        ("fillna", FunctionTransformer(lambda df: df.fillna("missing"))),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
    ])

    # Combine numeric + categorical
    preprocessor = ColumnTransformer(
        transformers=[("num", num_pipeline, numerical_features),
                      ("cat", cat_pipeline, categorical_features)],
        remainder="drop"
    )

    # Define and train the model
    model = Pipeline([("preprocessor", preprocessor),("classifier", LGBMClassifier(**params))])
    model.fit(x_train, y_train)

    # Log model to Feature Store + MLflow
    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=model_full_name
    )

print(f"Model logged in MLflow and registered in UC: {model_full_name}")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC  
# MAGIC #### Our model is now saved in Unity Catalog. 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-basic-model-uc_new.png?raw=true" width="700px" style="float: right"/>
# MAGIC
# MAGIC You can open the right menu to see the newly created "lightGBM" experiment, containing the model.
# MAGIC
# MAGIC In addition, the model also appears in Catalog Explorer, under the catalog we created earlier. This way, our tables and models are logically grouped together under the same catalog, making it easy to see all assets, whether data or models, associated with a catalog.
# MAGIC
# MAGIC <br>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC #### Table lineage
# MAGIC
# MAGIC Lineage is automatically captured and visible within Unity Catalog. It tracks all tables up to the model created.
# MAGIC
# MAGIC This makes it easy to track all your data usage, and downstream impact. If some PII information got leaked, or some incorrect data is loaded and detected by the Lakehouse Monitoring, it's then easy to track the potential impact.
# MAGIC
# MAGIC Note that this not only includes table and model, but also Notebooks, Dashboard, Jobs triggering the run etc.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-basic-fs-table-lineage.png?raw=true" width="700px">
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Move the model to Production
# MAGIC
# MAGIC Because we used the `registered_model_name` parameter, our model was automatically added to the registry. 
# MAGIC
# MAGIC We can now chose to move it in Production. 
# MAGIC
# MAGIC *Note that a typical ML pipeline would first run some tests & validation before doing moving the model as Production. We'll skip this step to focus on the Feature Engineering capabilities*

# COMMAND ----------

# DBTITLE 1,Move the last version in production
mlflow_client = MlflowClient()
# Use the MlflowClient to get a list of all versions for the registered model in Unity Catalog
all_versions = mlflow_client.search_model_versions(f"name='{model_full_name}'")
# Sort the list of versions by version number and get the latest version
latest_version = max([int(v.version) for v in all_versions])
# Use the MlflowClient to get the latest version of the registered model in Unity Catalog
latest_model = mlflow_client.get_model_version(model_full_name, str(latest_version))

# COMMAND ----------

#Move it in Production
production_alias = "production"
if len(latest_model.aliases) == 0 or latest_model.aliases[0] != production_alias:
  print(f"updating model {latest_model.version} to Production")
  mlflow_client.set_registered_model_alias(model_full_name, production_alias, version=latest_version)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3: Running inferences
# MAGIC
# MAGIC We are now ready to run inferences.
# MAGIC
# MAGIC In a real world setup, we would receive new data from our customers and have our job incrementally refreshing our customer features running in parallel. 
# MAGIC
# MAGIC To make the predictions, all we need to have is the primary keys for each feature loopup table. Feature Engineering in UC will automatically do the lookup for us as defined in the training steps.
# MAGIC
# MAGIC This is one of the great outcome using the Feature Engineering in UC: you know that your features will be used the same way for inference as training because it's being saved with your feature store metadata.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/2025Q4_01_image4.png" width="1000px">

# COMMAND ----------

# DBTITLE 1,Run inferences from a list of IDs
# Load the ids we want to forecast
## For sake of simplicity, we will just predict using the same ids as during training, but this could be a different pipeline
id_to_forecast = spark.table('travel_purchase').select("id","user_id","destination_id")

scored_df = fe.score_batch(model_uri=f"models:/{model_full_name}@{production_alias}", df=id_to_forecast, result_type="boolean")
display(scored_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Note that while we only selected a list of ID, we get back as result our prediction (is this user likely to book this travel `True`/`False`) and the full list of features automatically retrieved from our feature table.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Summary 
# MAGIC
# MAGIC We've seen a first basic example, creating two Feature Engineering table and training a model on top of that.
# MAGIC
# MAGIC Databricks Feature Engineering in Unity Catalog brings you a full traceability, knowing which model is using which feature in which notebook/job.
# MAGIC
# MAGIC It also simplify inferences by always making sure the same features will be used for model training and inference, always querying the same feature table based on your lookup keys.
# MAGIC
# MAGIC
# MAGIC ## Next Steps 
# MAGIC
# MAGIC We'll go more in details and introduce more feature engineering capabilities in the next demos:
# MAGIC
# MAGIC
# MAGIC Open the [02_Feature_store_advanced notebook]($./002_Feature_store_advanced) to explore more Feature Engineering in Unity Catalog benefits & capabilities:
# MAGIC - Combine multiple Feature Tables using **point-in-time lookup**.
# MAGIC - Create **Online Feature Tables** for real-time serving.
# MAGIC - Build and publish a **Feature Spec** for online inference.
# MAGIC - Deploy and query **Feature Serving Endpoints**.
# MAGIC - Serve a **UC-registered model** in real time with automatic feature lookup.
# MAGIC
