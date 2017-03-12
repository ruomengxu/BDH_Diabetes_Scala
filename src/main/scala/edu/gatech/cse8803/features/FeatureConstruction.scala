/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {

    val diag_feature = diagnostic.groupBy(diag=>(diag.patientID,diag.code)).map(diag=>(diag._1,diag._2.size.toDouble))
    diag_feature
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {

    val med_feature = medication.groupBy(med=>(med.patientID,med.medicine)).map(med=>(med._1,med._2.size.toDouble))
    med_feature
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {

    val lab_feature = labResult.map(lab=>((lab.patientID,lab.testName),lab.value)).groupByKey().map(lab=>(lab._1,lab._2.sum/lab._2.size))
    lab_feature
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val diag_feature = diagnostic.filter{case diag=>candiateCode.contains(diag.code)}.groupBy(diag=>(diag.patientID,diag.code)).map(diag=>(diag._1,diag._2.size.toDouble))
    diag_feature
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val med_feature = medication.filter{case med=>candidateMedication.contains(med.medicine)}.groupBy(med=>(med.patientID,med.medicine)).map(med=>(med._1,med._2.size.toDouble))
    med_feature
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lab_feature = labResult.filter{case lab=>candidateLab.contains(lab.testName)}.map(lab=>((lab.patientID,lab.testName),lab.value)).groupByKey().map(lab=>(lab._1,lab._2.sum/lab._2.size))
    lab_feature
  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()
    /** create a feature name to id map*/
    val feature_map = feature.map(_._1._2).distinct.collect.zipWithIndex.toMap
    val Feature_Map = sc.broadcast(feature_map)
    val length = Feature_Map.value.size
    /** transform input feature*/
    val feature_group = feature.map(f=>(f._1._1,(Feature_Map.value(f._1._2),f._2))).groupByKey()
    val result = feature_group.map(f=>(f._1,Vectors.sparse(length,f._2.toList)))
//    val temp =result.sortByKey()
//    temp.take(50).foreach(println)
    result
  }


}


