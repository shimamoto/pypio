
from __future__ import absolute_import

import atexit
import json
import os
import sys

from pypio.data import PEventStore
from pypio.utils import dict_to_scalamap, list_to_dict
from pypio.workflow import CleanupFunctions
from pyspark.sql import SparkSession


def init():
    global spark
    spark = SparkSession.builder.getOrCreate()
    global sc
    sc = spark.sparkContext
    global sqlContext
    sqlContext = spark._wrapped
    global p_event_store
    p_event_store = PEventStore(spark._jsparkSession, sqlContext)

    cleanup_functions = CleanupFunctions(sqlContext)
    atexit.register(lambda: cleanup_functions.run())
    atexit.register(lambda: sc.stop())
    print("Initialized pypio")


def find_events(app_name):
    return p_event_store.find(app_name)


def save_model(model, predict_columns):
    if not predict_columns:
        raise ValueError("predict_columns should have more than one value")
    serving_params = {"":{"columns":[]}}
    serving_params[""]["columns"].extend(predict_columns)

    if os.environ.get('PYSPARK_PYTHON') is None:
        # spark-submit
        d = list_to_dict(sys.argv[1:])
        engine_factory = d.get('--engine-factory')
        pio_env = list_to_dict([v for e in d['--env'].split(',') for v in e.split('=')])
    else:
        # pyspark
        engine_factory = None
        pio_env = {k: v for k, v in os.environ.items() if k.startswith('PIO_')}

    meta_storage = sc._jvm.org.apache.predictionio.data.storage.Storage.getMetaDataEngineInstances()

    meta = sc._jvm.org.apache.predictionio.data.storage.EngineInstance.apply(
        "",
        "INIT", # status
        sc._jvm.org.joda.time.DateTime.now(), # startTime
        sc._jvm.org.joda.time.DateTime.now(), # endTime
        engine_factory or "org.apache.predictionio.e2.engine.PythonEngine", # engineId
        "1", # engineVersion
        "default", # engineVariant
        engine_factory or "org.apache.predictionio.e2.engine.PythonEngine", # engineFactory
        "", # batch
        dict_to_scalamap(sc._jvm, pio_env), # env
        sc._jvm.scala.Predef.Map().empty(), # sparkConf
        "{\"\":{}}", # dataSourceParams
        "{\"\":{}}", # preparatorParams
        "[{\"default\":{}}]", # algorithmsParams
        json.dumps(serving_params) # servingParams
    )
    id = meta_storage.insert(meta)

    engine = sc._jvm.org.apache.predictionio.e2.engine.PythonEngine
    data = sc._jvm.org.apache.predictionio.data.storage.Model(id, engine.models(model._to_java()))
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

    return id


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
