
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
    engine_instance = sc._jvm.org.apache.predictionio.data.storage.Storage.getMetaDataEngineInstances()

    meta = sc._jvm.org.apache.predictionio.data.storage.EngineInstance.apply(
        "", "INIT", sc._jvm.org.joda.time.DateTime.now(), sc._jvm.org.joda.time.DateTime.now(),
        "", # engineId is the value of engineFactory in engine.json
        "", # engineVersion is the hash of engine directory
        "default", # engineVariant is the value of id in engine.json
        "", # engineFactory
        "", # batch
        sc._jvm.scala.Predef.Map().empty(), # env
        sc._jvm.scala.Predef.Map().empty(), # sparkConf
        "", "", "", ""
    )
    id = engine_instance.insert(meta)

    kryo = sc._jvm.org.apache.predictionio.workflow.KryoInstantiator.newKryoInjection()
    jl = sc._jvm.java.util.ArrayList()
    jl.add(model._to_java())
    data = sc._jvm.org.apache.predictionio.data.storage.Model(id, kryo.apply(sc._jvm.scala.collection.JavaConverters.asScalaBufferConverter(jl)))
    storage = sc._jvm.org.apache.predictionio.data.storage.Storage.getModelDataModels()
    storage.insert(data)

    engine_instance.update(
        sc._jvm.org.apache.predictionio.data.storage.EngineInstance.apply(
            id, "COMPLETED", meta.startTime(), sc._jvm.org.joda.time.DateTime.now(),
            meta.engineId(), meta.engineVersion(), meta.engineVariant(),
            meta.engineFactory(), meta.batch(), meta.env(), meta.sparkConf(),
            meta.dataSourceParams(), meta.preparatorParams(), meta.algorithmsParams(), meta.servingParams()
        )
    )

    CleanupFunctions(sqlContext).run()
    spark.stop()

def import_file(path=None, destination_frame=None, parse=True, header=0, sep=None, col_names=None, col_types=None,
                na_strings=None, pattern=None):
    print("TODO")
