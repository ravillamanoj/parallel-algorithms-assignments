/**
  * Created by manoj on 12/6/2016.
  */
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
object GraphxDemo {
  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("PA").setMaster("local[*]")

    val sc = new SparkContext(sconf)
    val input = sc.textFile("src/main/resources/input.csv")

    val splitInput = input.map(line => line.split(","))

    val triples = splitInput.map(line => (line(0), line(1), line(2)))

    val subjects = splitInput.map(line => line(0))

    val objects = splitInput.map(line => line(2))

    //val predicates = splitInput.map(line => line(1))

    val concepts = subjects.union(objects).distinct()

    val conceptId = concepts.zipWithIndex()

    val alignConceptSwitchId = conceptId.map(line => (line._2, line._1))
        val vertexRdd = alignConceptSwitchId

    val TriplesMap = triples.map(line => (line._1, (line._2, line._3)))

    val triplesJoin = TriplesMap.join(conceptId)

    val mapTriplesJoin = triplesJoin.map(line => ((line._2._2), (line._2._1)))

    val commandObjectJoin = mapTriplesJoin.map(line => (line._2._2, ((line._2._1), line._1)))

    val join_Objects = commandObjectJoin.join(conceptId)

    val mapObjectsJoin = join_Objects.map(line => (line._2._2, line._2._1))

    val edgeRdd = mapObjectsJoin.map(line => Edge(line._1, line._2._2, line._2._1))
    println("vertices")
    vertexRdd.foreach(println)
    println("edges:")
    edgeRdd.foreach(println)
    val graph = Graph(vertexRdd, edgeRdd)
    val ranks = graph.pageRank(0.1).vertices
    val vertexRanks = ranks.join(vertexRdd).map(line => (line._2._2, line._2._1))
    println("Page Rank:")
    vertexRanks.foreach(println)
    val connectedComponents = graph.connectedComponents().vertices
   println("CC:")
    connectedComponents.foreachPartition(println)
        val trCounting = graph.triangleCount().vertices

    println("Triangle counting:")

    trCounting.foreach(println)


  }

}
