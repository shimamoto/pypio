
from __future__ import absolute_import

import os
import sys

from pypio.data import PEventStore
from pyspark.files import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import utils


def init():
    global spark
    spark = SparkSession.builder.getOrCreate()
    global sc
    sc = spark.sparkContext
    global sqlContext
    sqlContext = spark._wrapped
    global p_event_store
    p_event_store = PEventStore(spark._jsparkSession, sqlContext)
    print("Initialized pypio")


def find_events(app_name):
    return p_event_store.find(app_name)


def save_model(model):
    meta_storage = sc._jvm.org.apache.predictionio.data.storage.Storage.getMetaDataEngineInstances()

    meta = sc._jvm.org.apache.predictionio.data.storage.EngineInstance.apply(
        "",
        "INIT", # status
        sc._jvm.org.joda.time.DateTime.now(), # startTime
        sc._jvm.org.joda.time.DateTime.now(), # endTime
        "org.apache.predictionio.e2.engine.PythonEngine", # engineId - the value of engineFactory in engine.json
        "1", # engineVersion - the hash of engine directory
        "default", # engineVariant - the value of id in engine.json
        "org.apache.predictionio.e2.engine.PythonEngine", # engineFactory
        "", # batch
        sc._jvm.scala.Predef.Map().empty(), # env
        sc._jvm.scala.Predef.Map().empty(), # sparkConf
        "", # dataSourceParams
        "", # preparatorParams
        "", # algorithmsParams
        "" # servingParams
    )
    id = meta_storage.insert(meta)

    kryo = sc._jvm.org.apache.predictionio.workflow.KryoInstantiator.newKryoInjection()
    jl = sc._jvm.java.util.ArrayList()
    jl.add(model._to_java())
    data = sc._jvm.org.apache.predictionio.data.storage.Model(id, kryo.apply(sc._jvm.scala.collection.JavaConverters.asScalaBufferConverter(jl)))
    model_storage = sc._jvm.org.apache.predictionio.data.storage.Storage.getModelDataModels()
    model_storage.insert(data)

    meta_storage.update(
        sc._jvm.org.apache.predictionio.data.storage.EngineInstance.apply(
            id, "COMPLETED", meta.startTime(), sc._jvm.org.joda.time.DateTime.now(),
            meta.engineId(), meta.engineVersion(), meta.engineVariant(),
            meta.engineFactory(), meta.batch(), meta.env(), meta.sparkConf(),
            meta.dataSourceParams(), meta.preparatorParams(), meta.algorithmsParams(), meta.servingParams()
        )
    )

def import_file(path, destination_frame=None, parse=True, header=None, sep=None, col_names=None, col_types=None,
                na_strings=None, pattern=None):
    basename = os.path.basename(path)

    if basename.endswith('.csv'):
        sc.addFile(path)
        df = spark.read.csv(SparkFiles.get(basename), header=header)
    elif basename.endswith('.json'):
        sc.addFile(path)
        df = spark.read.json(SparkFiles.get(basename))
    else:
        raise ValueError("")

#    df.foreach(lambda x: print(x.asDict(True)))


    print("TODO")
