#mrunit-hbase-extension
> Create simple unit tests when writing to HBase.
---

Reading from and writing to HBase is not recommended. It will just make your map reduce tasks slow. And yet... there are times when you still want to.

We found that the built in MapDriver and ReduceDriver just were not very helpful when working with HBase, so we wrote our own little wrapper with custom validation logic.

If you need to write large amounts of data to HBase, use a KeyValue or Put for the output value type and write to hdfs. Then use a bulk loader to actually put the data into HBase.

There is a sample test in the source showing how to make use of the HBaseMapDriver. This is the best place to go until I get better guidance here.
Shoot me questions on twitter @ExploreMqt.
Jim
