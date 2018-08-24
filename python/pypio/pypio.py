
from __future__ import absolute_import

from pypio.data import PEventStore
from pypio.workflow import CleanupFunctions
from pyspark.sql import SparkSession


def init():
    for k, v in globals().items():
        print(k, v)
    print("train spark-submit")
    global spark
    spark = SparkSession.builder.getOrCreate()
    global sc
    sc = spark.sparkContext
    global sqlContext
    sqlContext = spark._wrapped
    global p_event_store
    p_event_store = PEventStore(spark._jsparkSession, sqlContext)


def find(app_name):
    for k, v in globals().items():
        print(k, v)
    return p_event_store.find(app_name)


def save(model):
    print("Inserting persistent model")
    kryo = sc._jvm.org.apache.predictionio.workflow.KryoInstantiator.newKryoInjection()
    data = sc._jvm.org.apache.predictionio.data.storage.Model("test", kryo.apply(model._to_java()))
    storage = sc._jvm.org.apache.predictionio.data.storage.Storage.getModelDataModels()
    storage.insert(data)

    CleanupFunctions(sqlContext).run()
    spark.stop()

def import_file(path=None, destination_frame=None, parse=True, header=0, sep=None, col_names=None, col_types=None,
                na_strings=None, pattern=None):
    print("TODO")


