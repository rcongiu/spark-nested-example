name := "nested"

version := "1.0"

libraryDependencies  ++= Seq(
  "org.apache.spark"      %% "spark-core"      % "1.2.0"
  ,"org.apache.spark"      %% "spark-sql"       % "1.2.0"
  ,"org.apache.spark"      %% "spark-hive"      % "1.2.0"
  ,"com.databricks"        %% "spark-csv"     % "0.1.1"
)


