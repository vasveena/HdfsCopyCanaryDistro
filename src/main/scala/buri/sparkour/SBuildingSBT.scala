/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package buri.sparkour

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._

/**
 * A simple Scala application with an external dependency to
 * demonstrate building and submitting as well as creating an
 * assembly JAR.
 */

case class Random(suppkey: Double, name: String, address: String, nation: Int, phone: String, acctbal: Double, comment: String, test1: String, test2: String, test3: String, test4: Int, digest: String)

object SBuildingSBT {
	def main(args: Array[String]) {
                var j,l = 0
                //val result = (s"hdfs dfs -count /data/diff1/").!!.replaceAll(" +", " ").split(" ")
                val result = (s"hdfs dfs -count /data/diff1/").!!.replaceAll(" +", " ").split(" ")(1).toInt
                if (result > 0)
                {
                  j=result
                }
                val result2 = (s"hdfs dfs -count /data/diff2/").!!.replaceAll(" +", " ").split(" ")(1).toInt
                if (result2 > 0)
                {
                  l=result2
                }
		val spark = SparkSession.builder.appName("test").getOrCreate()
                import spark.implicits._
                val customSchema = StructType(Array(StructField("suppkey", DoubleType, true),StructField("name", StringType, true),StructField("address", StringType, true),StructField("nation", IntegerType, true),StructField("phone", StringType, true),StructField("acctbal", DoubleType, true),StructField("comment", StringType, true)))

                //val supplier = spark.read.format("csv").option("delimiter","|").schema(customSchema).load("s3n://vasveena-test-vanguard/bigtpc/supplier/")
                val supplier = spark.read.format("csv").option("delimiter","|").schema(customSchema).load("hdfs:///data/supplier/").repartition(400).cache
                //supplier.repartition(400)
                val supplier2 = supplier.withColumn("test1", when(col("acctbal") > 10, "less than 10").otherwise("more than 10"))
                  .withColumn("test2", when(col("acctbal") > 6000, "more than 6000").otherwise("more than 6000"))
                  .withColumn("test3", concat(lit("tester"), col("name")))
                  .withColumn("comment", concat(lit(" add to existing comment"), col("comment")))
                  .withColumn("test4", col("nation") + 100)
                  .withColumn("nation", col("nation") + 1000).cache
                val ds = supplier2.withColumn("digest", md5(concat(supplier2.columns.map(x => coalesce(col(x).cast("string"),lit(""))) : _*))).as[Random].cache
                //ds.repartition(1).write.mode("overwrite").parquet("/data/cntr/")
                ds.repartition(4).write.mode("overwrite").parquet("/data/folder"+j+"/output22/")

                val reader = spark.read.parquet("hdfs:///data/folder"+j+"/output22/").cache
                val ctrl = spark.read.parquet("hdfs:///data/cntr/").cache
                //val reader = spark.read.parquet("hdfs:///data/cntr/")
                val diff1 = reader.select($"digest").except(ctrl.select($"digest")).cache
                val diff2 = ctrl.select($"digest").except(reader.select($"digest")).cache
                val diff1count = diff1.count()
                val diff2count = diff2.count()
                println("Top 20 rows: ")
                reader.show(20)
                
                println("Count of diff1 between two dataframes: " +diff1count)
                if (diff1count > 0)
                {
                  j = j + 1
                  diff1.repartition(1).write.parquet("/data/diff1/folder"+j+"/")
                }
                println("Count of diff2 between two dataframes: " +diff2count)
                if (diff2count > 0)
                { 
                  l = l + 1
                  diff2.repartition(1).write.parquet("/data/diff2/folder"+l+"/")
                }
                
                val counter = reader.count()
                println("count is: " +counter)

		spark.stop()
	}
}
// scalastyle:on println
