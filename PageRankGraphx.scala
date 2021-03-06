/**
 * Created by manish on 2/11/15.
 */

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

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.apache.spark.bagel._

import scala.xml.{XML,NodeSeq}

/**
 * Run PageRank on XML Wikipedia dumps from http://wiki.freebase.com/wiki/WEX. Uses the "articles"
 * files from there, which contains one line per wiki article in a tab-separated format
 * (http://wiki.freebase.com/wiki/WEX/Documentation#articles).
 * Modified By: Manish Sapkota
 * Code samples have been used from scala examples
 */
object WikiPageRankGraphX {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: WikipediaPageRank <inputFile> <iterations> <outputpath>")
      System.exit(-1)
    }
    
    
    val inputFile = args(0)
    val iters = if (args.length > 0) args(1).toInt else 10
    val outputpath=args(2)

    val sparkConf = new SparkConf()
    sparkConf.setAppName("WikiPageRankGraphX")
    //sparkConf.setMaster("local[2]") // required to run on local machine
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
      val fullGraph=new String("")
      val outEdges = links.map(link => new String(link.text)).toArray // out links//links.foreach(link=> fullGraph + link+"\t")//
      val id = new String(title)
      (id, outEdges)
    })

    vertices=vertices.cache()

    println("Done parsing input file.")


    // Graph code refrence from http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html
    // create the vertices and the edges
    val gvertices: RDD[(VertexId,String)] = vertices.map{case(id,urls)=>
      (pageHash(id),id)
    }

  val edges: RDD[Edge[Double]] = vertices.flatMap { case (title, neighbors) =>
      neighbors.map { url =>
        val srcVid = pageHash(title)
        val dstVid = pageHash(url)
        Edge(srcVid, dstVid, 1.0)
      }
    }

    // got empty title when I did not remove the empty degrees
    val graph = Graph(gvertices, edges, "").subgraph(vpred = {(v, d) => d.nonEmpty}).cache

    // Run the page rank
    val t1 = System.currentTimeMillis
      val prGraph = graph.staticPageRank(iters).cache
    val t2 = System.currentTimeMillis
    
    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    val tophundred=titleAndPrGraph.vertices.top(100) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }

    sc.parallelize(tophundred).coalesce(1).saveAsTextFile(outputpath)

    tophundred.foreach(t => println(t._2._2 + ": " + t._2._1))


    println("==========================================================")
    println("Runtime for graphx page ranking: "+ (t2 - t1)/1000 + " secs")
    sc.stop()
  }

  // Hash function to assign an Id to each article
  def pageHash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }
}


