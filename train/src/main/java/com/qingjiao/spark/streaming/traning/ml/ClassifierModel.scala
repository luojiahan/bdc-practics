package com.qingjiao.spark.streaming.traning.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, MultilayerPerceptronClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ClassifierModel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("ClassifierModel").master("local[*]").getOrCreate()

    val file="/Users/kim/IdeaProjects/SparkTreaning0822/src/main/resources/data/Iris.csv"

    // 打开数据集文件
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file)

//    data.show()
//    data.printSchema()

    // 将label由String转换为Index数字
    val labelIndexer = new StringIndexer()
      .setInputCol("Species")
      .setOutputCol("label")
      .fit(data)

    // 将特征数据转换为向量
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.slice(1, 5))
      .setOutputCol("features")

    // 划分训练集和测试集
    val Array(train,test) = data.randomSplit(Array(0.7, 0.3), seed = 1024)

    // 创建模型
    // 决策树
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // 随机森林
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // 多层感知机：==> 1个输入层(神经元个数与输入元素保持一致)，2个隐藏层，1个输出层(神经元的个数与类别个数相同)
    val layers=Array(4,5,4,3)
    val mlp = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setLayers(layers)

    // 构建pipline：将label由String变为Index ==> 将特征转换为向量 => 模型
    // 决策树
    val pipelineDT = new Pipeline()
      .setStages(Array(labelIndexer, assembler, dt))

    // 随机森林
    val pipelineRF = new Pipeline()
      .setStages(Array(labelIndexer, assembler, rf))

    // 多层感知机
    val pipelineMLP = new Pipeline()
      .setStages(Array(labelIndexer, assembler, mlp))

    // 设置网格参数
    // 决策树
    val parmaDT = new ParamGridBuilder()
      .addGrid(dt.maxDepth, 2 to 10)
      .addGrid(dt.impurity, Array("gini", "entropy"))
      .build()

    // 随机森林
    val parmaRF = new ParamGridBuilder()
      .addGrid(rf.maxDepth, 2 to 10)
      .addGrid(rf.impurity, Array("gini", "entropy"))
      .addGrid(rf.numTrees,10 to 20)
      .build()

    // 多层感知机
    val parmaMLP = new ParamGridBuilder()
      .addGrid(mlp.layers, Array(Array(4,5,4,3),Array(4,10,5,3)))
      .addGrid(mlp.maxIter,10 to 30)
      .build()


    // 创建评估器
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    // 使用TrainValidationSplit进行模型训练和选择
    // 决策树
    val tvsDT = new TrainValidationSplit()
      .setEstimator(pipelineDT)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(parmaDT)
      .setTrainRatio(0.8)

    // 随机森林
    val tvsRF = new TrainValidationSplit()
      .setEstimator(pipelineRF)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(parmaRF)
      .setTrainRatio(0.8)

    // 决策树
    val tvsMLP = new TrainValidationSplit()
      .setEstimator(pipelineMLP)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(parmaMLP)
      .setTrainRatio(0.8)

    // 训练模型
    // 决策树模型
    val modelDT = tvsDT.fit(train)
    // 随机森林模型
    val modelRF = tvsRF.fit(train)
    // 多层感知机模型
    val modelMLP = tvsMLP.fit(train)

    // 所有模型
    val models=Array(modelDT,modelRF,modelMLP)

    // 保存每个模型的评估结果
    val scores = new ArrayBuffer[Double]()

    // 选择最优模型进行评估
    for (model <- models) {
      val bestModel = model.bestModel.asInstanceOf[PipelineModel]
      val predictions = bestModel.transform(test)
      val accuracy = evaluator.evaluate(predictions)
      // 将评估结果存入scores
      scores+=accuracy
    }

    // 输出同一个数据集上不同模型的表现
    println(s"DecisionTree Accuracy:${scores(0)},RandomForest Accuracy:${scores(1)},MLP Accuracy:${scores(2)}")


    spark.stop()
  }

}
