# Databricks notebook source
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set the number of days and the start and end values for answer relevance
num_days = 30
start_value = 4
end_value = 3.8

# Generate dates
start_date = datetime.today()
dates = [start_date + timedelta(days=x) for x in range(num_days)]

# Generate linearly decreasing values for answer relevance
linear_values = np.linspace(start_value, end_value, num_days)

# Add some random fluctuation to answer relevance
np.random.seed(0)  # for reproducibility
fluctuation = np.random.normal(0, 0.02, num_days)  # adjust 0.2 for more/less fluctuation
answer_relevance_values = linear_values + fluctuation

# Generate stable values for toxicity around 0.1%
toxicity_values = np.random.normal(0.15, 0.015, num_days)  # slight fluctuation around 0.1%

# Generate values for session count
session_count_values = []
for date in dates:
    if date.weekday() >= 5:  # weekends (5 for Saturday, 6 for Sunday)
        sessions = np.random.normal(500, 50)  # lower session count on weekends
    else:
        sessions = np.random.normal(1000, 100)  # normal session count on weekdays
    session_count_values.append(sessions)

# Generate values for human interaction required
human_interaction_values = [np.random.randint(5, 15) if date.weekday() < 5 else 0 for date in dates]

# Generate values for professionalism with small random fluctuation
professionalism_values = np.random.normal(4, 0.25, num_days)  # fluctuation around 4

# Create the DataFrame
df2 = pd.DataFrame({
    'date': dates,
    'average_answer_relevance': answer_relevance_values,
    'toxicity': toxicity_values,
    'session_count': session_count_values,
    'human_interaction_required': human_interaction_values,
    'professionalism': professionalism_values
})

# Clip values to ensure they stay within reasonable ranges
df2['average_answer_relevance'] = df2['average_answer_relevance'].clip(2, 5)
df2['toxicity'] = df2['toxicity'].clip(0, 1)
df2['session_count'] = df2['session_count'].clip(0, None)
df2['professionalism'] = df2['professionalism'].clip(3.5, 4.5)  # keeping professionalism within a reasonable range
df2['model_version'] = 2
# Print the DataFrame
display(df2)

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set the number of days and the start and end values for answer relevance
num_days = 30
start_value = 4
end_value = 2

# Generate dates
start_date = datetime.today()
dates = [start_date + timedelta(days=x) for x in range(num_days)]

# Generate linearly decreasing values for answer relevance
linear_values = np.linspace(start_value, end_value, num_days)

# Add some random fluctuation to answer relevance
np.random.seed(0)  # for reproducibility
fluctuation = np.random.normal(0, 0.2, num_days)  # adjust 0.2 for more/less fluctuation
answer_relevance_values = linear_values + fluctuation

# Generate stable values for toxicity around 0.15%
toxicity_values = np.random.normal(0.1, 0.005, num_days)  # slight fluctuation around 0.1%

# Generate values for session count
session_count_values = []
for date in dates:
    if date.weekday() >= 5:  # weekends (5 for Saturday, 6 for Sunday)
        sessions = np.random.normal(500, 50)  # lower session count on weekends
    else:
        sessions = np.random.normal(1000, 100)  # normal session count on weekdays
    session_count_values.append(sessions)

# Generate values for human interaction required
human_interaction_values = [np.random.randint(45, 115) if date.weekday() < 5 else 0 for date in dates]

# Generate values for professionalism with small random fluctuation
professionalism_values = np.random.normal(4, 0.25, num_days)  # fluctuation around 4

# Create the DataFrame
df = pd.DataFrame({
    'date': dates,
    'average_answer_relevance': answer_relevance_values,
    'toxicity': toxicity_values,
    'session_count': session_count_values,
    'human_interaction_required': human_interaction_values,
    'professionalism': professionalism_values
})

# Clip values to ensure they stay within reasonable ranges
df['average_answer_relevance'] = df['average_answer_relevance'].clip(2, 5)
df['toxicity'] = df['toxicity'].clip(0, 1)
df['session_count'] = df['session_count'].clip(0, None)
df['professionalism'] = df['professionalism'].clip(3.5, 4.5)  # keeping professionalism within a reasonable range
df['model_version'] = 1
# Print the DataFrame
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dbdemos.chatbot.chatbot_evaluation_llm_as_a_judge
# MAGIC

# COMMAND ----------

spark.createDataFrame(pd.concat([df,df2])).write.mode('overwrite').option("mergeSchema", "true").saveAsTable('dbdemos.chatbot.chatbot_evaluation_llm_as_a_judge')

# COMMAND ----------

spark.table('dbdemos.chatbot.chatbot_evaluation_llm_as_a_judge').display()
