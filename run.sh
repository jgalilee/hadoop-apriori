ant clean && ant jar
rm -rf ./output/*
rm input/apriori.state
hadoop jar HadoopApriori.jar com.jgalilee.hadoop.apriori.driver.Driver \
  input/apriori.state \
  input/transactions.txt \
  output \
  3 \
  10 \
  2
cat output/**/*
