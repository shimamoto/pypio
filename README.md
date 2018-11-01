# pypio

Prerequisites
- Python code to specify for the `--main-py-file` option
  - pypio.init()
  - event_df = pypio.find('BHPApp')
  - pypio.save(model)
- template.json
```
{"pio": {"version": { "min": "0.14.0-SNAPSHOT" }}}
```
- engine.json
```
{
  "id": "default",
  "description": "Default settings",
  "engineFactory": "org.apache.predictionio.e2.engine.PythonEngine",
  "algorithms": [
    {
      "name": "default",
      "params": {
        "name": "BHPApp"
      }
    }
  ]
}
```


memo
- Spark Packages
```
bin/pyspark --master spark://spark-master:7077 --packages ml.combust.mleap:mleap-spark_2.11:0.12.0
```
