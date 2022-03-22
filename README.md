# hudi-spark-utilities-plus

Refactor the Hudi-based Spark library. 

### **Features**

1.**CDC Ingestion**

- [Binary Logs Ingestion](./docs/binlog/01.Binary Logs Ingestion.md)

2.**Documents Ingestion**


- [MongoDB（dev）](./docs/document/01.MongoDB Ingestion.md)


- [Elasticsearch（dev）](./docs/document/02.Elasticsearch Ingestion.md)


### **How to Build？**


```shell
mvn clean package -pl [model] -am -Dmaven.test.skip=true
```


### **Requirements**

The library currently supports the following versions of components：

- Scala：2.12.x

- Spark：3.1.x

- Hudi：0.9.0

