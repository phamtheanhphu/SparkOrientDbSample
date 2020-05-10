import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.{OrientDynaElementIterable, OrientGraph, OrientVertex}
import org.apache.spark._
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SparkGraphXPageRankDBLP {

  val orientDbProtocol: String = "remote"
  val orientDbHost: String = "localhost"
  val orientDbDatabase: String = "dblp"
  val orientDbUsername: String = "root"
  val orientDbPassword: String = "123abc"
  val numberOfTakenPapers: Int = 500

  def main(args: Array[String]): Unit = {

    //Setup OrientDb environment
    val orientDbUri: String = s"${orientDbProtocol}:${orientDbHost}/${orientDbDatabase}"
    val dblpOrientDbGraph: OrientGraph = new OrientGraph(orientDbUri, orientDbUsername, orientDbPassword)

    //Setup Spark environment
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkGraphX").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")

    var addedPapers = mutable.HashMap[String, String]()
    var dblpVertices = ArrayBuffer[(VertexId, String)]()
    var dblpEdges = ArrayBuffer[Edge[String]]()

    try {

      //Get papers from OrientDb
      val results: OrientDynaElementIterable = dblpOrientDbGraph
        .command(new OCommandSQL(s"SELECT FROM Paper LIMIT ${numberOfTakenPapers.toString}"))
        .execute()

      results.forEach(v => {
        //Loop through each returned paper
        val paper = v.asInstanceOf[OrientVertex]
        if (paper.getProperty("indexedId") != null && paper.getProperty("title") != null) {
          val paperIndexedId = paper.getProperty("indexedId").toString
          val paperTitle = paper.getProperty("title").toString
          dblpVertices += ((paperIndexedId.toLong, paperTitle))
          addedPapers += (paperIndexedId -> paperTitle)
          val paperReferencesIterator = paper.getEdges(Direction.IN, "reference").iterator()

          //Fetching target referred papers of current paper
          while (paperReferencesIterator.hasNext) {
            val referredPaper = paperReferencesIterator.next().getVertex(Direction.OUT).asInstanceOf[OrientVertex]
            if (referredPaper.getProperty("indexedId") != null && referredPaper.getProperty("title") != null) {
              val referredPaperIndexedId = referredPaper.getProperty("indexedId").toString
              val referredPaperTitle = referredPaper.getProperty("title").toString
              if (!addedPapers.keySet.contains(referredPaperIndexedId)) {
                dblpVertices += ((referredPaperIndexedId.toLong, referredPaperTitle))
                addedPapers += (referredPaperIndexedId -> referredPaperTitle)
              }
              dblpEdges += Edge(paperIndexedId.toLong, referredPaperIndexedId.toLong, "reference")
            }
          }
        }
      })

      println(s"Reading total: [${dblpVertices.count(z => true)}] vertices and [${dblpEdges.count(z => true)}] edges from OrientDb !")

      //Creating graphX object from current vertices & edges of DBLP
      val rddVertices: RDD[(VertexId, String)] = sparkContext.parallelize(dblpVertices)
      val rddEdgs: RDD[Edge[String]] = sparkContext.parallelize(dblpEdges)
      val dblpGraphX = Graph(rddVertices, rddEdgs)

      //Applying PageRank algorithm to rank nodes
      val tolConstant = 0.0001
      val rankedVertices = dblpGraphX.pageRank(tolConstant).vertices

      //Take top-10 ranked papers & print out
      val top10RankedPapers = dblpGraphX.vertices.join(rankedVertices).sortBy(rankedPaper => rankedPaper._2._2, false).take(10)
      top10RankedPapers.foreach(rankedPaper => {
        println(s" - Paper: [${rankedPaper._1}][${rankedPaper._2._1}] - score: [${rankedPaper._2._2}]")
      })

    } catch {
      //catching errors & print out
      case ex: Exception => println(ex.getMessage)
    }
    finally {
      //close the current OrientDb's connection
      dblpOrientDbGraph.shutdown()
    }
  }
}
