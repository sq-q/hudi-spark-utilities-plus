# hudi-spark-utilities-plus

Refactor the Hudi-based Spark library. 

### **Features**

1.**CDC Ingestion**

- Binary Logs Ingestion（MySQL）

2.**Documents Ingestion**

- MongoDB

- Elasticsearch

3.**File Ingestion**

- Excel

4.**RDB Ingestion**

- JDBC


### **How to Build？**


```shell
mvn clean package -pl [model] -am -Dmaven.test.skip=true
```


### **Requirements**

The library currently supports the following versions of components：

- Scala：2.12.x

- Spark：3.1.x

- Hudi：0.9.0

