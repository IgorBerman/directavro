package org.apache.avro.mapreduce;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.google.common.io.Files;

public class DirectAvroKeyValueOutputFormatTest implements Serializable {
	protected transient JavaSparkContext sc;
	protected File tempDir;

	@Before
	public void setUp() throws Exception {
		tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		SparkConf sparkConf = new SparkConf();
		// sparkConf.set(SPARK_SERIALIZER, KryoSerializer.class.getName());
		// sparkConf.set(SPARK_KRYO_REGISTRATOR, Registrator.class.getName());
		sparkConf.set("spark.serializer", KryoSerializer.class.getName());

		sparkConf.setMaster("local[*]");
		sparkConf.setAppName(getClass().getSimpleName());
		sparkConf.set("spark.local.dir", tempDir + "/spark");
		sc = new JavaSparkContext(sparkConf);
		sc.setCheckpointDir(tempDir + "/checkpoint/");
	}

	@After
	public void tearDown() throws Exception {
		if (sc != null) {
			sc.stop();
		}
		FileUtils.deleteQuietly(tempDir);
		sc = null;
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> classOf(T instance) {
		return (Class<T>) instance.getClass();
	}

	@Test
	public void test() throws IOException {
		Schema longSchema = Schema.create(Type.LONG);
		Job outputJob = Job.getInstance(sc.hadoopConfiguration());
		AvroJob.setOutputKeySchema(outputJob, longSchema);
		AvroJob.setOutputValueSchema(outputJob, longSchema);
		JavaPairRDD<AvroKey<Long>, AvroValue<Long>> data = sc.parallelizePairs(Arrays.asList(new Tuple2<AvroKey<Long>, AvroValue<Long>>(new AvroKey<Long>(1L),new AvroValue<Long>(1L))));
		String path = tempDir + "/data/";
		data.saveAsNewAPIHadoopFile(path, 
				classOf(new AvroKey<Long>()), 
				classOf(new AvroValue<Long>()),
				classOf(new DirectAvroKeyValueOutputFormat<AvroKey<Long>, AvroValue<Long>>()),
				outputJob.getConfiguration());
		
		
		Job inputJob = Job.getInstance(sc.hadoopConfiguration());
		AvroJob.setInputKeySchema(inputJob, longSchema);
		AvroJob.setInputValueSchema(inputJob, longSchema);
		JavaPairRDD<AvroKey<Long>, AvroValue<Long>> fileRDD = sc.newAPIHadoopFile(path + "*.avro",
				classOf(new AvroKeyValueInputFormat<Long, Long>()), 
				classOf(new AvroKey<Long>()),
				classOf(new AvroValue<Long>()), 
				inputJob.getConfiguration() 
				);
		
		assertEquals(1, fileRDD.collect().size());
	}

}
