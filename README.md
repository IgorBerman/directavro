# Description
Direct file output committer for Avro key value format.

Not yet tested in production. 

Solves performance problem when working with newHadoopApi and s3. 

DirectFileOutputCommiter MUST NOT be used with speculation or append mode. 

Writes directly to destination directory instead of using _temporary folder(since moving in s3 file system is copying files one-by-one)

## How to use
For basic usage see DirectAvroKeyValueOutputFormatTest.java. 

Test uses spark, but the code itself is probably good for MR too

