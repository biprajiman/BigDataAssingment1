/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Modified By: Manish Sapkota

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.bagel._


import scala.xml.{XML,NodeSeq}

/**
 * Run PageRank on XML Wikipedia dumps from http://wiki.freebase.com/wiki/WEX. Uses the "articles"
 * files from there, which contains one line per wiki article in a tab-separated format
 * (http://wiki.freebase.com/wiki/WEX/Documentation#articles).
 * Modified By: Manish Sapkota
 * Code samples have been used from scala examples
 */
object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: WikipediaPageRank <inputFile> <iterations> <outputpath>")
      System.exit(-1)
    }
    val sparkConf = new SparkConf()
    sparkConf.setAppName("WikipediaPageRank")

    val inputFile = args(0)
    val iters = if (args.length > 0) args(1).toInt else 10
    val outputpath=args(2)

    sparkConf.setAppName("WikipediaPageRank")
    //sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // Parse the Wikipedia page data into a graph
    // Original code adapted from scala examples
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices.")

    println("Parsing input file...")
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val links =
        if (body == "\\N") {
          NodeSeq.Empty
        } else {
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \"" + title + "\" has malformed XML in body:\n" + body)
            NodeSeq.Empty
          }
        }
      val outEdges = links.map(link => new String(link.text)).toArray // out links
      val id = new String(title)
      (id, outEdges)
    })

     vertices=vertices.cache();

     println("Done parsing input file.")

    var ranks = vertices.mapValues(v => 1.0) // assign rank of 1 to all of the url

    val t1 = System.currentTimeMillis

    // iterate over and modify the ranks
    for (i <- 1 to iters) {
      val conts = vertices.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => {
                        //println(url)
                         (url, rank / size)
        })
      }
      ranks = conts.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val t2 = System.currentTimeMillis

    ranks.cache()


    // sort the output to get the results
    val output = ranks.map(item => item.swap).sortByKey(false, 1).map(item => item.swap).take(100)

    // Save the output to the file
    sc.parallelize(output).coalesce(1).saveAsTextFile(outputpath)

    // get top 100
    for(i<-0 to 99)
    {
        val tup=output(i)
        println(tup._1+" : "+ tup._2)
    }

    println("==========================================================")
    println("Runtime for page ranking: "+ (t2 - t1)/1000 + " secs")

    sc.stop()
  }
}


