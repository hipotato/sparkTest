 /home/hbase/spark/spark-2.1.0-bin-hadoop2.6/bin/spark-submit --class org.potato.poiRec.poiMatchPro  \
--master yarn-cluster \
--jars /home/hbase/spark/conf/commons-beanutils-1.9.2.jar,/home/hbase/spark/conf/commons-logging-1.1.3.jar,/home/hbase/spark/conf/ezmorph-1.0.6.jar,/home/hbase/spark/conf/json-lib-2.4-jdk15.jar,/home/hbase/spark/conf/hbase-util-1.2.7-SNAPSHOT.jar,/home/hbase/spark/conf/hanlp-portable-1.2.8.jar,/home/hbase/spark/conf/mysql-connector-java-5.1.40.jar \
--files /home/hbase/spark/spark-2.1.0-bin-hadoop2.6/conf/hive-site.xml \
--executor-memory 1G \
--driver-memory 1G \
--executor-cores 3 \
--num-executors 40 \
--packages com.squareup.okhttp3:okhttp:3.5.0,org.mongodb.spark:mongo-spark-connector_2.10:2.1.1 \