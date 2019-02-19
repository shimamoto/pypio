memo
- Spark Packages
```
bin/pyspark --master spark://spark-master:7077 --packages ml.combust.mleap:mleap-spark_2.11:0.12.0
```

- akka-cluster
```
sbt docker:publishLocal
kubectl create -f k8s/rbac.yml
kubectl create -f k8s/deployment.yml
kubectl create -f k8s/service.yml
```
