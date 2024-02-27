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
# MAGIC Multiple version of this demo are available, each version introducing a new concept and capabilities. We recommend following them 1 by 1 as.
# MAGIC
# MAGIC In this first version, we'll cover the basics:
# MAGIC
# MAGIC  - Ingest our data and save them as a feature table within Unity Catalog
# MAGIC  - Create a Feature Lookup with multiple tables
# MAGIC  - Train your model using the Feature Engineering Client
# MAGIC  - Register your best model and promote it into Production
# MAGIC  - Perform batch scoring
# MAGIC  
# MAGIC  
# MAGIC For more detail on the Feature Engineering in Unity Catalog, open <a href="https://api-docs.databricks.com/python/feature-engineering/latest" target="_blank">the documentation</a>.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Building a propensity score to book travels & hotels
# MAGIC
# MAGIC Fot this demo, we'll step in the shoes of a Travel Agency offering deals in their website.
# MAGIC
# MAGIC Our job is to increase our revenue by boosting the amount of purchases, pushing personalized offer based on what our customers are the most likely to buy.
# MAGIC
# MAGIC In order to personalize offer recommendation in our application, we have have been asked as a Data Scientist to create the TraveRecommendationModel that predicts the probability of purchasing a given travel. 
# MAGIC
# MAGIC For this first version, we'll use a single data source: **Travel Purchased by users**
# MAGIC
# MAGIC
# MAGIC We're going to make a basic single Feature Table that contains all existing features (**`clicked`** or **`price`**) and a few generated one(derivated from the timestamp). 
# MAGIC
# MAGIC We'll then use these features to train our baseline model, and to predict whether a user is likely to purchased a travel on our Website.
# MAGIC
# MAGIC The goal of this first use case is to understand what feature tables are and how they work. 
# MAGIC
# MAGIC With the following demos we will increase the complexity of the use case.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init-basic $catalog="feat_eng"

# COMMAND ----------

# DBTITLE 1,Let's review our silver table we'll use to create our features
# MAGIC %sql SELECT * FROM travel_purchase

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
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_creation.png" alt="Feature Engineering Table Creation" width="500px" style="margin-left: 10px; float: right"/>
# MAGIC
# MAGIC Our first step is to create our Feature Engineering table.
# MAGIC
# MAGIC We will load data from the silver table `travel_purchase` and create features from these values. 
# MAGIC
# MAGIC In this first version, we'll transform the timestamp into multiple features that our model will be able to understand. 
# MAGIC
# MAGIC In addition, we will drop the label from the table as we don't want it to leak our features when we do our training.
# MAGIC
# MAGIC To create the feature table, we'll use the `FeatureEngineeringClient.create_table`. 
# MAGIC
# MAGIC Under the hood, this will create a Delta Table to save our information. 
# MAGIC
# MAGIC These steps would typically live in a separate job that we call to refresh our features when new data lands in the silver table.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Compute the features 
# MAGIC
# MAGIC Let's create the features that we'll save in our Feature Table. We'll keep it simple this first example, changing the data type and add extra columns based on the date.
# MAGIC
# MAGIC This transformation would typically be part of a job used to refresh our feature, triggered for model training and inference so that the features are computed with the same code.

# COMMAND ----------

# DBTITLE 1,Create our features using Pandas API on top of spark
import numpy as np

#Get our table and switch to pandas APIs
df = spark.table('travel_purchase').pandas_api()

#Add features from the time variable 
def add_time_features(df):
    # Extract day of the week, day of the month, and hour from the ts column
    df['day_of_week'] = df['ts'].dt.dayofweek
    df['day_of_month'] = df['ts'].dt.day
    df['hour'] = df['ts'].dt.hour
    
    # Calculate sin and cos values for the day of the week, day of the month, and hour
    df['day_of_week_sin'] = np.sin(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_week_cos'] = np.cos(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_month_sin'] = np.sin(df['day_of_month'] * (2 * np.pi / 30))
    df['day_of_month_cos'] = np.cos(df['day_of_month'] * (2 * np.pi / 30))
    df['hour_sin'] = np.sin(df['hour'] * (2 * np.pi / 24))
    df['hour_cos'] = np.cos(df['hour'] * (2 * np.pi / 24))
    df = df.drop(['ts', 'day_of_week', 'day_of_month', 'hour'], axis=1)
    return df

df["clicked"] = df["clicked"].astype(int)
df = add_time_features(df)

# COMMAND ----------

# DBTITLE 1,Labels shouldn't be part of the feature to avoid leaking result to our models
#Drop the label column from our dataframe
df = df.drop("purchased", axis=1)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Feature Engineering Table
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

fe = FeatureEngineeringClient()

fe_table_name = f"travel_recommender_basic"
fe.create_table(
    name=fe_table_name, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["id"],
    df=df.to_spark(),
    description="Travel purchases dataset with purchase timestamp",
    tags={"team":"analytics"}
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Alternatively, you can first **`create_table`** with a schema only, and populate data to the feature table with **`fs.write_table`**. **`fs.write_table`** supports both **`overwrite`** and **`merge`** modes (based on the primary key).
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```
# MAGIC fe.create_table(
# MAGIC     name=fe_table_name,
# MAGIC     primary_keys=["destination_id"],
# MAGIC     schema=destination_features_df.schema,
# MAGIC     description="Destination Popularity Features",
# MAGIC )
# MAGIC
# MAGIC fe.write_table(
# MAGIC     name=fe_table_name,
# MAGIC     df=destination_features_df,
# MAGIC     mode="overwrite"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/jeannefukumaru/feat_eng_demo_images/main/01_basic_feat_table.png" style="float: right" width="700px">
# MAGIC
# MAGIC #### Our table is now ready!
# MAGIC
# MAGIC We can explore the created feature engineering table using the Unity Catalog Explorer. 
# MAGIC
# MAGIC From within the Explorer, select the catalog `feat_eng` and browse the tables in the dropdown.
# MAGIC
# MAGIC We can view some sample data from the `travel_recommender_basic` table that was just created.
# MAGIC
# MAGIC Additionally, we can also use the `fe.get_table()` method to get metadata associated with our newly created table. 

# COMMAND ----------

fe_get_table = fe.get_table(name=fe_table_name)
print(f"Feature Table in UC= {fe_table_name}. Description: {fe_get_table.description}")
print("The table contains those features: ", fe_get_table.features)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Table lineage
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/jeannefukumaru/feat_eng_demo_images/main/01_basic_feat_table_lineage.png" style="float: right" width="700px">
# MAGIC
# MAGIC Note also the section **`Lineage`**. This section indicates the upstream producers and downstream consumers of this table. This is one of the benefits of Feature Engineering in Unity Catalog (UC). When feature tables are also UC tables, we can use the UC lineage graph to audit our feature engineering pipelines and the models produced by these pipelines. 
# MAGIC
# MAGIC Consumers include which notebook produced the feature table.
# MAGIC
# MAGIC For now, there are no models that are consuming this table. Let's create our first model.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2: Train a model with FS 
# MAGIC
# MAGIC
# MAGIC We'll now train a ML model using the feature stored in our datasets.
# MAGIC
# MAGIC * First we need to build or training dataset. We'll need to provide a list of destination id (used as our feature table primary key) and the associated label we want to predict. We'll then retrieve the features from the feature table using a Feature Lookup list which will join the data based on the lookup key **`id`**
# MAGIC * We'll then train our model using these features
# MAGIC * Finally, we'll deploy this model in production.
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_training.png" style="margin-left: 10px" width="1200px">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Build the training dataset 
# MAGIC
# MAGIC Let's start by building the dataset, retrieving features from our feature table.

# COMMAND ----------

# DBTITLE 1,Get our list of id & labels
training_dataset_key = spark.table('travel_purchase').select("id", "purchased")
display(training_dataset_key)

# COMMAND ----------

# DBTITLE 1,Retrieve the features from the feature table
model_feature_lookups = [
      FeatureLookup(
          table_name=fe_table_name,
          lookup_key=["id"],
          #feature_names=["price"], # if you dont specify here the FS will take all your features apart from primary_keys 
      )
]
# fe.create_training_set will look up features in model_feature_lookups with matched key from training_labels_df
training_set = fe.create_training_set(
    df=training_dataset_key, # joining the original Dataset, with our FeatureLookupTable
    feature_lookups=model_feature_lookups,
    exclude_columns=["user_id", "id", "booking_date"], # exclude features we won't use in our model
    label='purchased',
)

training_pd = training_set.load_df().toPandas()
display(training_pd)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Training our baseline model 
# MAGIC
# MAGIC Note that for our first basic example, the feature used are very limited and our model will very likely not be efficient, but we won't focus on the model performance.
# MAGIC
# MAGIC The following steps will be a basic LGBM model. For a more complete ML training example including hyperparameter tuning, we recommend using Databricks Auto ML and exploring the generated notebooks.
# MAGIC
# MAGIC Note that to log the model, we'll use the `FeatureEngineeringClient.log_model(...)` function and not the usual `mlflow.skearn.log_model(...)`. This will capture all the feature dependencies & lineage for us and update the feature table data.

# COMMAND ----------

# DBTITLE 1,Split the dataset
X_train = training_pd.drop('purchased', axis=1)
Y_train = training_pd['purchased'].values.ravel()
x_train, x_val,  y_train, y_val = train_test_split(X_train, Y_train, test_size=0.10, stratify=Y_train)

# COMMAND ----------

# DBTITLE 1,Train a model using the training dataset & log it using the Feature Engineering client
mlflow.sklearn.autolog(log_input_examples=True,silent=True)

with mlflow.start_run(run_name="lightGBM") as run:
  #Define our LGBM model
  numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("standardizer", StandardScaler())])
  one_hot_pipeline = Pipeline(steps=[("one_hot_encoder", OneHotEncoder(handle_unknown="ignore"))])
  preprocessor = ColumnTransformer([("numerical", numerical_pipeline, ["clicked", "price"]),
                                    ("onehot", one_hot_pipeline, ["clicked", "destination_id"])], 
                                    remainder="passthrough", sparse_threshold=0)
  model = Pipeline([
      ("preprocessor", preprocessor),
      ("classifier", LGBMClassifier(**params)),
  ])

  #Train the model
  model.fit(x_train, y_train)  

  #log the model. Note that we're using the fs client to do that
  fe.log_model(
              model=model, # object of your model
              artifact_path="model", #name of the Artifact under MlFlow
              flavor=mlflow.sklearn, # flavour of the model (our LightGBM model has a SkLearn Flavour)
              training_set=training_set, # training set you used to train your model with AutoML
              registered_model_name=model_registry_name, # register your best model
          )

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC #### Our model is now saved in MLFlow. 
# MAGIC
# MAGIC You can open the right menu to see the newly created "lightGBM" experiment, containing the model.
# MAGIC
# MAGIC In addition, the model also appears in Catalog Explorer, under the `feat_eng` catalog we created earlier. This way, our tables and models are logically grouped together under the same catalog, making it easy to see all assets, whether data or models, associated with a catalog.
# MAGIC
# MAGIC <br>
# MAGIC <img src="https://raw.githubusercontent.com/jeannefukumaru/feat_eng_demo_images/main/01_basic_model_catalog.png" width="500px"/>

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Model metadata
# MAGIC From the Catalog Explorer, we can also view more detailed model information, such as its version, its metrics and whether it has been aliased as production-ready.
# MAGIC <br>
# MAGIC <img src="https://raw.githubusercontent.com/jeannefukumaru/feat_eng_demo_images/main/01_basic_model_version.png" width="1000px"/>

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

# TODO: Define this somewhere else 
model_registry_name = "feat_eng.dbdemos_fs_travel_shared.dbdemos_fs_travel_shared_model"

# COMMAND ----------

# DBTITLE 1,Move the last version in production
mlflow_client = MlflowClient()
# Use the MlflowClient to get a list of all versions for the registered model in Unity Catalog
all_versions = mlflow_client.search_model_versions(f"name='{model_registry_name}'")
# Sort the list of versions by version number and get the latest version
latest_version = max([int(v.version) for v in all_versions])
# Use the MlflowClient to get the latest version of the registered model in Unity Catalog
latest_model = mlflow_client.get_model_version(model_registry_name, str(latest_version))

# COMMAND ----------

#TODO: abstract away model name
if latest_model.current_stage != 'Production':
  model_name = "dbdemos_fs_travel_shared_model"
  destination_alias = "Production"
  print(f"updating model {latest_model.version} to Production")
  mlflow_client.set_registered_model_alias(model_name, destination_alias, version=latest_version)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3: Running inferences
# MAGIC
# MAGIC We are now ready to run inferences.
# MAGIC
# MAGIC In a real world setup, we would receive new data from our customers and have our job incrementally refreshing our customer features running in parallel. 
# MAGIC
# MAGIC To make the predictions, all we need to have is our customer ID. Feature Engineering in UC will automatically do the lookup for us as defined in the training steps.
# MAGIC
# MAGIC This is one of the great outcome using the Feature Engineering in UC: you know that your features will be used the same way for inference as training because it's being saved with your feature store metadata.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_inference.png" width="1000px">

# COMMAND ----------

# DBTITLE 1,Run inferences from a list of IDs
# Load the ids we want to forecast
## For sake of simplicity, we will just predict using the same ids as during training, but this could be a different pipeline
id_to_forecast = spark.table('travel_purchase').select("id")

scored_df = fe.score_batch(model_uri=f"models:/{model_name}@Production", df=id_to_forecast, result_type="boolean")
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
# MAGIC We've seen a first basic example, creating a Feature Engineering table and training a model on top of that.
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
# MAGIC - Use multiple Feature Tables
# MAGIC - Create a Feature Table from a Streaming table 
# MAGIC - Calculate new features based on the destination coordinates and user's on the fly 
# MAGIC - Publish your Feature Tables Online with a Key/Value feature store (Redis, DynamoDB, CosmoDB...), allowing realtime feature lookup  
# MAGIC - Serve your model in Streaming and using Online Feature Stores Tables 
# MAGIC
# MAGIC
# MAGIC Open the [02_Feature_store_advanced notebook]($./02_Feature_store_advanced) to explore more Feature Engineering in Unity Catalog benefits & capabilities:
# MAGIC - Multiple lookup tables
# MAGIC - Leveraging Databricks Automl to get a more advanced model
# MAGIC - Using point in time lookups
