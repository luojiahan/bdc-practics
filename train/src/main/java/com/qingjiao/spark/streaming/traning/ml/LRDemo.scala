package com.qingjiao.spark.streaming.traning.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

object LRDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LRDemo").master("local[*]").getOrCreate()

    val file="/Users/kim/IdeaProjects/SparkTreaning0822/src/main/resources/data/boston.csv"

    val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load(file).na.drop()

    // 数据集划分
    val Array(train,test) = data.randomSplit(Array(0.7, 0.3), seed = 1024)

    // 将特征转换为向量
    val assembler = new VectorAssembler().setInputCols(data.columns.slice(0, 13)).setOutputCol("rawFeatures")

    // 对特征进行归一化操作
    val scaler = new MinMaxScaler().setInputCol("rawFeatures").setOutputCol("features")

    // 创建线性回归模型
    val lr = new LinearRegression().setLabelCol("MEDV").setFeaturesCol("features")

    // 创建pipline
    val pipeline = new Pipeline().setStages(Array(assembler, scaler, lr))

    // 网格搜索参数
    val params = new ParamGridBuilder()
      .addGrid(lr.regParam, 0.1 to 1.0 by 0.1)
      .addGrid(lr.elasticNetParam, 0.1 to 1.0 by 0.1)
      .build()

    // 创建评估器
    val evaluator = new RegressionEvaluator()
      .setLabelCol("MEDV")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // 交叉验证
    val validator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(params)
      .setNumFolds(3)

    // 使用训练集训练模型
    val model = validator.fit(train)
    // 获取最优模型
    val bestModel = model.bestModel.asInstanceOf[PipelineModel]

    // 使用测试集验证模型
    val predictions = bestModel.transform(test)
    val rmse = evaluator.evaluate(predictions)

    println(s"test RMSE:$rmse")

    spark.stop()
  }

}
