
from __future__ import absolute_import

import sys

from pypio.data import PEventStore
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


def find(app_name):
    return p_event_store.find(app_name)


def save(model):
    engine = sc._jvm.org.apache.predictionio.e2.engine.PythonEngine
    engine.model().set(model._to_java())
    args = sys.argv
#    args.append("--engine-factory")
#    args.append(engine.getClass().getName())
    main_args = utils.toJArray(sc._gateway, sc._gateway.jvm.String, args)
    create_workflow = sc._jvm.org.apache.predictionio.workflow.CreateWorkflow
    spark.stop()
    create_workflow.main(main_args)


def import_file(path=None, destination_frame=None, parse=True, header=0, sep=None, col_names=None, col_types=None,
                na_strings=None, pattern=None):
    print("TODO")
