# Apache Beam





## **Concepts**

**Pipeline:** A pipeline encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data.

**PCollection:** A PCollection is equivalent to RDDD of Spark. The data set can be bounded, meaning it comes from a fixed source like a file (Batch), or unbounded, meaning it comes from a continuously (Streaming).

**PTransform:** PTransform represents a data processing operation or a step in our pipeline.

Every PTransform takes one or more PCollection objects as input, performs a processing function that you provide on the elements of that PCollection, and produces zero or more output PCollection objects

Example: ParDo, filter, flatten, combine, etc.




## Install libraries

We need to install the next [libraries](https://github.com/BenRamo06/ApacheBeam/blob/Dev_ApacheBeam/Prerequisites/libraries.py) to create pipelines in Apache Beam



## Create Pipeline


Remember, A pipeline encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data.

Create a pipeline can be [two forms](https://github.com/BenRamo06/ApacheBeam/blob/Dev_ApacheBeam/Pipeline/01.-Create_pipeline.py):

* Sentence **with**
* Sentence **run**




## 
