/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.clustering

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the purity of clustering.
   * Purity is defined as
   *             \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return purity
   */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    /**
     * TODO: Remove the placeholder and implement your code here
     */
    clusterAssignmentAndLabel.cache()
    val N = clusterAssignmentAndLabel.count()
    val cluster_class_id = clusterAssignmentAndLabel.zipWithUniqueId()
    val cluster_id = cluster_class_id.map(t=>(t._1._1,t._2)).groupByKey().map(t=>(0,t))
    val class_id = cluster_class_id.map(t=>(t._1._2,t._2)).groupByKey().map(t=>(0,t))
    val cluster_class = cluster_id.leftOuterJoin(class_id)
    val temp = cluster_class.map{case (x,(y,Some(z))) => (y._1,(y._2.toSet.intersect(z._2.toSet)).size) }.groupByKey().map(t=>(t._1,t._2.max))
    val result=temp.map(t=>t._2).sum()/N
    result
  }
}
