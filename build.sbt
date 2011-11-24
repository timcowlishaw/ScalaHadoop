name := "ScalaHadoop"

version := "1.0"

scalaVersion := "2.9.1"

resolvers += "Ryan Rawson's ASF Repository for legacy Thrift version depended on by HBase" at "http://people.apache.org/~rawson/repo" 

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "0.20.205.0"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.90.4"

libraryDependencies += "commons-logging" % "commons-logging" % "1.1.1"
