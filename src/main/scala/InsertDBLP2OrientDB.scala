import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object InsertDBLP2OrientDB {

  val dblpDatasetFilePath: String = "[...DBLPOnlyCitationOct19.txt...]"

  val PAPER_TITLE_PREFIX: String = "#*"
  val PAPER_AUTHORS_PREFIX: String = "#@"
  val PAPER_YEAR_PREFIX: String = "#t"
  val PAPER_VENUE_PREFIX: String = "#c"
  val REFERRED_INDEXED_ID_PREFIX: String = "#%"
  val PAPER_INDEXED_ID_PREFIX: String = "#index"

  val orientDbProtocol: String = "remote"
  val orientDbHost: String = "localhost"
  val orientDbDatabase: String = "dblp"
  val orientDbUsername: String = "root"
  val orientDbPassword: String = "123abc"

  def main(args: Array[String]): Unit = {

    val sourceFile = Source.fromFile(dblpDatasetFilePath)

    var paperTitle: String = null
    var paperAuthors: ListBuffer[String] = ListBuffer()
    var paperIndexId: String = null
    var paperReferences: ListBuffer[String] = ListBuffer()

    var PaperIdTitleMap = mutable.HashMap[String, String]()
    var AuthorNameIdMap = mutable.HashMap[String, String]()
    var PaperIdAuthorIdMap = mutable.HashMap[String, String]()
    var PaperIdReferredIdMap = mutable.HashMap[String, String]()

    var currentAuthorIdCount = 1

    sourceFile.getLines().foreach(line => {

      if (line.startsWith(PAPER_INDEXED_ID_PREFIX)) {
        paperIndexId = line.replace(PAPER_INDEXED_ID_PREFIX, "")
        //ending of paper's information
        if (paperIndexId != null && paperTitle != null) {

          PaperIdTitleMap += (paperIndexId -> paperTitle)

          if (paperAuthors.nonEmpty) {
            paperAuthors.foreach(authorName => {
              if (!AuthorNameIdMap.contains(authorName)) {
                AuthorNameIdMap += (authorName -> currentAuthorIdCount.toString)
                PaperIdAuthorIdMap += (paperIndexId -> currentAuthorIdCount.toString)
                currentAuthorIdCount += 1
              } else {
                PaperIdAuthorIdMap += (paperIndexId -> AuthorNameIdMap(authorName))
              }
            })
          }

          if (paperReferences.nonEmpty) {
            paperReferences.foreach(referredPaperId => {
              PaperIdReferredIdMap += (paperIndexId -> referredPaperId)
            })
          }

          //clear previous paper data
          paperTitle = null
          paperIndexId = null
          paperAuthors = ListBuffer()
          paperReferences = ListBuffer()
        }
      } else if (line.startsWith(PAPER_TITLE_PREFIX)) {
        paperTitle = line.replace(PAPER_TITLE_PREFIX, "")
      } else if (line.startsWith(PAPER_AUTHORS_PREFIX)) {
        val authors = line.replace(PAPER_AUTHORS_PREFIX, "").split(',')
        if (authors.length > 0) authors.foreach(author => paperAuthors += author)
      } else if (line.startsWith(REFERRED_INDEXED_ID_PREFIX)) {
        val referredIndexedPaperId = line.replace(REFERRED_INDEXED_ID_PREFIX, "")
        paperReferences += referredIndexedPaperId
      }

    })

    println("Total parsed papers: " + PaperIdTitleMap.keySet.count(z => true))
    println("Total parsed authors: " + AuthorNameIdMap.keySet.count(z => true))
    println("Total parsed references: " + PaperIdReferredIdMap.keySet.count(z => true))

    println("Insert data to OrientDb...")

    //Setup OrientDb environment
    val orientDbUri: String = s"${orientDbProtocol}:${orientDbHost}/${orientDbDatabase}"
    val dblpOrientDbGraph: OrientGraph = new OrientGraph(orientDbUri, orientDbUsername, orientDbPassword)

    var PaperIdVertexIdMap = mutable.HashMap[String, AnyRef]()
    var AuthorIdVertexIdMap = mutable.HashMap[String, AnyRef]()

    try {

      if (dblpOrientDbGraph.getVertexType("Paper") == null) {
        val person: OrientVertexType = dblpOrientDbGraph.createVertexType("Paper")
        person.createProperty("title", OType.STRING)
        person.createProperty("indexedId", OType.STRING)
      }

      if (dblpOrientDbGraph.getVertexType("Author") == null) {
        val person: OrientVertexType = dblpOrientDbGraph.createVertexType("Author")
        person.createProperty("name", OType.STRING)
        person.createProperty("indexedId", OType.STRING)
      }

      var count = 0

      //Clear all current data first
      /*graph.command(new OCommandSQL("DELETE VERTEX V")).execute()
      graph.command(new OCommandSQL("DELETE EDGE E")).execute()*/

      //Adding paper vertices
      PaperIdTitleMap.keySet.foreach(paperIndexId => {
        val targetPaperVertex: Vertex = dblpOrientDbGraph.addVertex("class:Paper", Nil: _*)
        targetPaperVertex.setProperty("indexedId", paperIndexId)
        targetPaperVertex.setProperty("title", PaperIdTitleMap(paperIndexId))
        PaperIdVertexIdMap += (paperIndexId -> targetPaperVertex.getId)
        if (count % 1000 == 0) dblpOrientDbGraph.commit()
        count += 1
      })

      //Adding author vertices
      AuthorNameIdMap.keySet.foreach(authorName => {
        val targetAuthorVertex: Vertex = dblpOrientDbGraph.addVertex("class:Author", Nil: _*)
        targetAuthorVertex.setProperty("indexedId", AuthorNameIdMap(authorName))
        targetAuthorVertex.setProperty("name", authorName)
        AuthorIdVertexIdMap += (AuthorNameIdMap(authorName) -> targetAuthorVertex.getId)
        if (count % 1000 == 0) dblpOrientDbGraph.commit()
        count += 1
      })

      //Adding author-paper edges
      PaperIdAuthorIdMap.keySet.foreach(paperIndexId => {
        val targetPaperVertex = dblpOrientDbGraph.getVertex(PaperIdVertexIdMap(paperIndexId))
        val targetAuthorVertex = dblpOrientDbGraph.getVertex(AuthorIdVertexIdMap(PaperIdAuthorIdMap(paperIndexId)))
        if (targetPaperVertex != null && targetAuthorVertex != null) {
          dblpOrientDbGraph.addEdge(null, targetAuthorVertex, targetPaperVertex, "author_of")
          if (count % 1000 == 0) dblpOrientDbGraph.commit()
          count += 1
        }
      })

      //Adding paper-paper edges
      PaperIdReferredIdMap.keySet.foreach(paperIndexId => {
        val targetPaperVertex = dblpOrientDbGraph.getVertex(PaperIdVertexIdMap(paperIndexId))
        val targetReferredPaperVertex = dblpOrientDbGraph.getVertex(PaperIdVertexIdMap(PaperIdReferredIdMap(paperIndexId)))
        if (targetPaperVertex != null && targetReferredPaperVertex != null) {
          dblpOrientDbGraph.addEdge(null, targetPaperVertex, targetReferredPaperVertex, "reference")
          if (count % 1000 == 0) dblpOrientDbGraph.commit()
          count += 1
        }
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
