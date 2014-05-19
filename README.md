# Hadoop Apriori

This is an implementation of the Apriori algorithm using Hadoop MapReduce. It
has not yet been tested on large input datasets and is not thought to scale up.

## Usage

```
$ hadoop jar HadoopApriori.jar com.jgalilee.hadoop.apriori.AprioriDriver \
    -D apriori.iterations.max=100 \
    -D minsup=3 \
    /DATASET /out
```

## Options

 * apriori.iterations.max - Maximum number of iterations, default -1.
 * apriori.iterations.minsup - Minimum support threshold, default 0.

## Example Dataset

There is a simple sample dataset included as part of the repository. It is a
copy of the dataset described by Tan, Steinbach, Kumar.

 * 1  Bread Milk
 * 2  Beer Diaper Eggs
 * 3  Beer Bread Coke Diaper Milk
 * 4  Beer Bread Diaper Milk
 * 5  Bread Coke Diaper Milk

## Licence

Copyright 2014 Jack Galilee

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

