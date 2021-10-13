# Apache Beam

Apache Beam is an open source, unified model for defining both batch and streaming data-parallel processing pipelines. Using one of the open source Beam SDKs, you build a program that defines the pipeline.

Beam is particularly useful for embarrassingly parallel data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel. You can also use Beam for Extract, Transform, and Load (ETL) tasks and pure data integration. These tasks are useful for moving data between different storage media and data sources, transforming data into a more desirable format, or loading data onto a new system.

Also

![a](https://github.com/BenRamo06/ApacheBeam/blob/Dev_ApacheBeam/images/apache%20beam.png)


## **Concepts**

**Pipeline:** A pipeline encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data.

**PCollection:** A PCollection is equivalent to RDDD of Spark. The data set can be bounded, meaning it comes from a fixed source like a file (Batch), or unbounded, meaning it comes from a continuously (Streaming).

**PTransform:** PTransform represents a data processing operation or a step in our pipeline.

Every PTransform takes one or more PCollection objects as input, performs a processing function that you provide on the elements of that PCollection, and produces zero or more output PCollection objects

Example: ParDo, filter, flatten, combine, etc.




## Install libraries

We need to install the next [libraries](https://github.com/BenRamo06/ApacheBeam/blob/Dev_ApacheBeam/Prerequisites/libraries.py) to create pipelines in Apache Beam



## Create Pipeline

Create a pipeline can be [two forms](https://github.com/BenRamo06/ApacheBeam/blob/Dev_ApacheBeam/Pipeline/01.-Create_Pipeline.py):

* Sentence **with**
* Sentence **run**


## Create PCollection

We could create a PCollection from: list, set or dictionaries

A PCollection is immutable. Once created, you cannot add, remove, or change individual elements. A Beam Transform might process each element of a PCollection and generate new pipeline data (as a new PCollection), but it does not consume or modify the original input collection.

