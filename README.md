# Description
Direct file output committer for Avro key value format.

Tested in production. So far so good.

Solves performance problem when working with newHadoopApi and s3. 

DirectFileOutputCommiter MUST NOT be used with speculation or append mode. 

Writes directly to destination directory instead of using _temporary folder(since moving in s3 file system is copying files one-by-one)

## References
1. [Intro about committers:](http://johnjianfang.blogspot.co.il/2014/09/outputcommitter-in-hadoop-two.html)
2. [This project is based on gist of Aaron Davidson](https://gist.github.com/aarondav/c513916e72101bbe14ec)

## How to use
For basic usage see DirectAvroKeyValueOutputFormatTest.java. 

Test uses spark, but the code itself is probably good for MR too

