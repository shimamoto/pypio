
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import IndexToString
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.feature import StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[2]:


from pypio import pypio


# In[3]:


pypio.init()


# In[4]:


event_df = pypio.find('BHPApp')


# In[5]:


event_df.show(5)


# In[6]:


def get_field_type(name):
    return 'double'

field_names = (event_df
            .select(explode("fields"))
            .select("key")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect())
field_names.sort()
exprs = [col("fields").getItem(k).cast(get_field_type(k)).alias(k) for k in field_names]
data_df = event_df.select(*exprs)
data_df = data_df.withColumnRenamed("MEDV", "label")


# In[7]:


data_df.show(5)


# In[8]:


(train_df, test_df) = data_df.randomSplit([0.9, 0.1])


# In[9]:


featureAssembler = VectorAssembler(inputCols=[x for x in field_names if x != 'MEDV'],
                                   outputCol="rawFeatures")
scaler = StandardScaler(inputCol="rawFeatures", outputCol="features")
clf = RandomForestRegressor(featuresCol="features", labelCol="label", predictionCol="prediction",
                            maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                            maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                            impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20,
                            featureSubsetStrategy="auto")
pipeline = Pipeline(stages=[featureAssembler, scaler, clf])


# In[10]:


model = pipeline.fit(train_df)


# In[11]:


predict_df = model.transform(test_df)


# In[12]:


predict_df.select("prediction", "label").show(5)


# In[13]:


evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predict_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


pypio.save(model)
