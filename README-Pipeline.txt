## Create Pipeline
continuous_features

categorical_features

rom pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression

## Create indexers for the categorical features
indexers = [StringIndexer(inputCol=c, outputCol="{}_idx".format(c)) for c in categorical_features]

## encode the categorical features
encoders = [ OneHotEncoderEstimator(
      inputCol=idx.getOutputCol(),
      outputCol="{0}_enc".format(idx.getOutputCol())) for idx in indexers]

## Create vectors for all features categorical and continuous

assembler = VectorAssembler(
  inputCols=[enc.getOutputCol() for enc in estimator] + continuous_features,
  outputCol="features")

## Initialize the linear model
lrModel = LogisticRegression( maxIter = 10 )


## Create the pipeline with sequence of activities
#pipeline = Pipeline( stages=indexers + encoders + [assembler, lrModel ])

pipeline = Pipeline( stages= [indexers, Estimator, assembler, lrModel ])

