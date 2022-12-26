import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import java.io.File


object Main {
  def main(args: Array[String]): Unit = {
    try {
      ////////////////////// LOADING THE DATA /////////////////////////////
      val spark = SparkSession
        .builder()
        .appName("Flights_Delay_Calculator")
        .config("spark.master", "local")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      def readCSV(path: String): DataFrame = {
        // ****** We are going to read (and combine) all CSV files from different years in a single Dataframe **********
        var df_readCSV = spark.emptyDataFrame
        val csv_directory = new File(path)
        var filesList = List[File]()
        // we check it is a valid directory
        if (csv_directory.isDirectory){
          val csvFilesInResourceDir = csv_directory.listFiles.filter { file =>
            file.isFile && file.getName.endsWith(".csv")
          }
          filesList = csvFilesInResourceDir.toList
        }
        // This will prevent the 'real' DataFrame "df" created out of this function from being empty
        if (filesList.isEmpty) {
          System.err.println("No files to read. You can pass a local path as parameter or include flight datasets in the folder where you are launching the application.")
          sys.exit(1)
        }
        // iterate all the files contained in the list and create the DataFrame
        filesList.foreach(single_file =>
          if (df_readCSV.isEmpty) {
            df_readCSV = spark.read.format("csv").option("header", "true").load(single_file.toString)
            println("Using file: " + single_file.toString)
          } else {
            val df_single_file = spark.read.format("csv").option("header", "true").load(single_file.toString)
            df_readCSV = df_readCSV.union(df_single_file)
            println("Using file: " + single_file.toString)
          }
        )
        df_readCSV
      }

      // CREATING THE DATAFRAME
      // You can choose between passing a local folder in the arguments or using CSVs from the launching directory"
      var df = spark.emptyDataFrame
      if (args.length != 0) {
        // The first argument must be a path pointing to the directory where we have the CSV files
        df = readCSV(args(0))
      }else{
        // If we do not pass an argument the application is going to use CSVs from the launching directory
        println("WARNING: the current directory will be used to search for flight datasets. ")
        val cwd = System.getProperty("user.dir")
        val currentWorkingDirectory = new String(cwd)
        df = readCSV(currentWorkingDirectory)
      }

      ////////////////////// PREPROCESSING THE DATA /////////////////////////////
      // Drop the Forbidden variables and Deleting the variables that are not going to be used
      // Forbidden variables
      df = df.drop(
        "ArrTime",
        "ActualElapsedTime",
        "AirTime",
        "TaxiIn",
        "Diverted",
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay",
        // Variables dropped by our own criteria
        // The properly variable analysis have been done with the correlation matrix externally to this document (see report)
        "Cancelled",
        "CancellationCode",
        "CRSDepTime",
        "UniqueCarrier",
        "TailNum",
        "Year", "Month", "DayofMonth", "DayOfWeek"
      )

      // Transform the columns into numeric type
      val available_columns = df.columns
      val columns_ListBuffer = available_columns.toBuffer
      columns_ListBuffer -= ("FlightNum", "Origin", "Dest", "ArrDelay") // these three will be transformed in another way
      columns_ListBuffer.foreach(col_name =>
        df = df.withColumn(col_name, col(col_name).cast("double"))
      )

      // FlightNum feature will be considered categorical
      df = df.withColumn("FlightNum", col("FlightNum").cast(IntegerType))

      // ArrDelay: target column to predict must be named "label" to make the cross validation work
      df = df.withColumn("label", col("ArrDelay").cast("double"))
      df = df.drop("ArrDelay")

      // Null values are filled with 0's
      df = df.na.fill(0)

      // SOME VISUALIZATIONS OF THE DATAFRAME:
      println("Schema: ")
      df.printSchema()
      println("First rows: ")
      df.show(10, truncate = false)

      // Label encoder for Origin and Destiny:
      val indexer = new StringIndexer()
        .setInputCols(Array("Origin", "Dest"))
        .setOutputCols(Array("OriginIdx", "DestIdx"))
        .setHandleInvalid("skip")

      // Split between train and test
      val Array(train, test) = df.randomSplit(Array(0.7, 0.3))

      train.cache()
      test.cache()

      ////////////////////// CREATING THE MODEL /////////////////////////////
      // Several models are created and trained with the dataframe
      val assembler = new VectorAssembler()
        .setInputCols(Array("DepTime", "CRSArrTime", "FlightNum", "CRSElapsedTime", "DepDelay", "Distance", "TaxiOut", "OriginIdx", "DestIdx"))
        .setOutputCol("features")

      val evaluator = new RegressionEvaluator()

      // Model 1: Linear Regression
      val lr = new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setMaxIter(10)
        .setElasticNetParam(0.8)

      // pipeline to join the assembler and lr model
      val pipelineLr = new Pipeline()
        .setStages(Array(indexer, assembler, lr))

      // all possible hyper-parameters to try
      val paramGridLr = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.01))
        .addGrid(lr.fitIntercept)
        .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
        .build()

      // Model 2: Decision Tree Regressor

      // compute how many categorical values we have
      val maxCat = Math.max(df.agg(countDistinct("Origin")).first().getLong(0),
        df.agg(countDistinct("Dest")).first().getLong(0))

      val dtr = new DecisionTreeRegressor()
        .setMaxBins(maxCat.toInt)
        .setMinInstancesPerNode(1)
        .setMaxDepth(10)

      // pipeline to join the assembler and dtr model
      val pipelineDtr = new Pipeline()
        .setStages(Array(indexer, assembler, dtr))

      // all possible hyper-parameters to try
      val paramGridDtr = new ParamGridBuilder()
        .addGrid(dtr.minInstancesPerNode, Array(1, 10))
        .addGrid(dtr.maxDepth, Array(5, 10))
        .build()

      // Model 3: Random Forest Regressor
      val rfr = new RandomForestRegressor()
        .setNumTrees(3)
        .setMaxBins(maxCat.toInt)

      // pipeline to join the assembler and rfr model
      val pipelineRfr = new Pipeline()
        .setStages(Array(indexer, assembler, rfr))

      // all possible hyper-parameters to try
      val paramGridRfr = new ParamGridBuilder()
        .addGrid(rfr.numTrees, Array(2, 3))
        .build()

      ////////////////////// TRAIN & VALIDATING THE MODEL /////////////////////////////

      // Train Model 1
      val cvLr = new CrossValidator()
        .setEstimator(pipelineLr)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGridLr)
        .setNumFolds(3)
        .setParallelism(2)

      val lrModel = cvLr.fit(train)

      // Validate Model 1
      val predictionsLr = lrModel.transform(test)

      println("Linear Regression:")
      val r2lr = evaluator.setMetricName("r2").evaluate(predictionsLr)
      val rmselr = evaluator.setMetricName("rmse").evaluate(predictionsLr)
      println("R2: " + r2lr)
      println("RMSE: " + rmselr)

      // Train Model 2
      val cvDtr = new CrossValidator()
        .setEstimator(pipelineDtr)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGridDtr)
        .setNumFolds(2)
        .setParallelism(2)

      val dtrModel = cvDtr.fit(train)

      // Validate Model 2
      val predictionsDtr = dtrModel.transform(test)

      println("\nDecision Tree Regressor:")
      val r2dtr = evaluator.setMetricName("r2").evaluate(predictionsDtr)
      val rmsedtr = evaluator.setMetricName("rmse").evaluate(predictionsDtr)
      println("R2: " + r2dtr)
      println("RMSE: " + rmsedtr)

      // Model 3 will use the optimal parameters found for a single tree in Model 2
      val bestDtr = dtrModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[DecisionTreeRegressionModel]
      rfr.setMaxDepth(bestDtr.getMaxDepth)
      rfr.setMinInstancesPerNode(bestDtr.getMinInstancesPerNode)

      // Train Model 3
      val cvRfr = new CrossValidator()
        .setEstimator(pipelineRfr)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGridRfr)
        .setNumFolds(3)
        .setParallelism(2)

      val rfrModel = cvRfr.fit(train)

      // Validate Model 3
      val predictionsRfr = rfrModel.transform(test)

      println("\nRandom Forest Regressor")
      val r2rfr = evaluator.setMetricName("r2").evaluate(predictionsRfr)
      val rmserfr = evaluator.setMetricName("rmse").evaluate(predictionsRfr)
      println("R2: " + r2rfr)
      println("RMSE: " + rmserfr)

      val resultsR2 = Map(
        "Linear Regression" -> r2lr,
        "Decision Tree Regressor" -> r2dtr,
        "Random Forest Regressor" -> r2rfr
      )
      val resultsRMSE = Map(
        "Linear Regression" -> rmselr,
        "Decision Tree Regressor" -> rmsedtr,
        "Random Forest Regressor" -> rmserfr
      )
      println("\nBest global R2 (" + resultsR2.maxBy(_._2)._1 + "): " + resultsR2.values.max)
      println("Best global RMSE (" + resultsRMSE.minBy(_._2)._1 + "): " + resultsRMSE.values.min)


    } catch {
      case e: Exception => println("Exception: Something have gone wrong.\n" + e.toString)
    }
  }
}

