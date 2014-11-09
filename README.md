# Hadoop Apriori

Brute force Apriori algorithm implementation using Hadoop. This algorithm does
not continue and build the association rules.

## Usage

*home input output minsup max number*

```
hadoop jar HadoopApriori.jar com.jgalilee.hadoop.apriori.driver.Driver \
  input/apriori.state \
  input/transactions.txt \
  output \
  3 \
  10 \
  2
```

* home - Path to a filename iteration state can be written each iteration.
* input - Path to the input transaction data.
* output - Path to write the output for iteration n - i.e. output/n
* minsup - Minimum support candidate itemsets to be deemed frequent itemsets.
* max  - Maximum number of iterations to run for.
* number - Number of reducers to suggest to the Hadoop job.

## Assumptions

Input data is assumed to be string representations in integer format -

tid \t 1 \s 2 \s ...

Where \t is a tab, and \s is a space. The number of duplicate items or order is
not important. This will be resolved during the map phase.

## Dependencies

* Hadoop 2.2.0

## Licence

```
The MIT License (MIT)

Copyright (c) 2014 Jack Galilee

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```
