/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import java.text.SimpleDateFormat
import java.util

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

import scala.collection



object T2dmPhenotype {
  
  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
      "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
      "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {

    val sc = medication.sparkContext
    medication.cache()
    labResult.cache()
    diagnostic.cache()

    /** Hard code the criteria */
    val Related_DX = Set("790.21","790.22","790.2","790.29","648.81","648.82","648.83","648.84","648.0","648.00","648.01","648.02","648.03","648.04","791.5","277.7","V77.1","256.4")
    /** Find CASE Patients */

    val med_map = medication.map(med=>(med.patientID,med.medicine))
    val diag_map = diagnostic.map(diag=>(diag.patientID,diag.code))
    val t1_patient_id = diag_map.filter{case (x,y) => T1DM_DX.contains(y)}.keys.collect.toSet
    val patient_id_filter2 = diag_map.filter{case (x,y) => !t1_patient_id.contains(x) && T2DM_DX.contains(y) }.keys.collect.toSet
//    println(patient_id_filter2.size)
    val t1_med_patient = med_map.filter{case (x,y) => T1DM_MED.contains(y.toLowerCase())}.keys.collect.toSet
    val patient_id_filter3_n = patient_id_filter2.diff(t1_med_patient)
//    println(patient_id_filter3_n.size)
    val patient_id_filter3_y = patient_id_filter2.intersect(t1_med_patient)
//    println(patient_id_filter3_y.size)
    val t2_med_patient = med_map.filter{case (x,y) => T2DM_MED.contains(y.toLowerCase())}.keys.collect.toSet
    val patient_id_filter4_n = patient_id_filter3_y.diff(t2_med_patient)
//    println(patient_id_filter4_n.size)
    val patient_id_filter4_y = patient_id_filter3_y.intersect(t2_med_patient)
//    println(patient_id_filter4_y.size)
    val med_filter4_T1_MD = medication.filter{case med => patient_id_filter4_y.contains(med.patientID) && T1DM_MED.contains(med.medicine.toLowerCase())}.map(med => (med.patientID,med.date)).groupByKey().map(x => (x._1,x._2.min))
    val med_filter4_T2_MD = medication.filter{case med => patient_id_filter4_y.contains(med.patientID) && T2DM_MED.contains(med.medicine.toLowerCase())}.map(med => (med.patientID,med.date)).groupByKey().map(x => (x._1,x._2.min))
    val temp = med_filter4_T1_MD.leftOuterJoin(med_filter4_T2_MD)
    val patient_id_filter5 = temp.flatMap{case (x,(y,Some(z))) => Some(x,y,z) }.filter{case t => (t._2.compareTo(t._3)>0)}.map(x => (x._1,1)).keys.collect.toSet
//    println(patient_id_filter5.size)
    val case_id = patient_id_filter3_n.union(patient_id_filter4_n).union(patient_id_filter5)
    val casePatients = sc.parallelize(case_id.toSeq).map(p => (p,1))

    /** Find CONTROL Patients */
    val lab_filter1 = labResult.filter{case lab => lab.testName.contains("glucose")}.map(lab=>lab.patientID).distinct().collect.toSet
//    println(lab_filter1.size)
    val lab_filter2_temp = labResult.filter{case lab=> abnormal(lab)}.map(lab=>lab.patientID).distinct().collect().toSet
    val lab_filter2 = lab_filter1.diff(lab_filter2_temp)
//    println(lab_filter2.size)
    val lab_filter3 = diagnostic.filter{case diag =>  (Related_DX.contains(diag.code) || diag.code.contains("250")) && lab_filter2.contains(diag.patientID)}.map(diag => diag.patientID).collect.toSet
    val control_id = lab_filter2.diff(lab_filter3)
//    println(control_id.size)
    val controlPatients = sc.parallelize(control_id.toSeq).map(p=>(p,2))

    /** Find OTHER Patients */
    val others = sc.parallelize(diag_map.keys.collect.toSet.diff(casePatients.keys.collect.toSet).diff(controlPatients.keys.collect.toSet).toSeq).map(p=>(p,3))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
//    println(casePatients.count())
//    println(controlPatients.count())
//    println(others.count())
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }
  def abnormal(item: LabResult): Boolean = {
    item.testName match {
      case "hba1c" => item.value >= 6
      case "hemoglobin a1c" => item.value >= 6
      case "fasting glucose" => item.value >= 110
      case "fasting blood glucose" => item.value >= 110
      case "fasting plasma glucose" => item.value >= 110
      case "glucose" => item.value > 110
      case "glucose, serum" => item.value > 110
      case _ => false

    }
  }



}
