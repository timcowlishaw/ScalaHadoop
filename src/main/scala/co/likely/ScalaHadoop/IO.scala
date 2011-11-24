package co.likely.ScalaHadoop

import org.apache.hadoop.mapreduce._;
import org.apache.hadoop.io._;

import scala.reflect.Manifest;

object IO {

  trait Input[K, V] {
     val inFormatClass : java.lang.Class[_ <: InputFormat[K,V]]
     def setup(job : Job) : Unit;
  }

  trait Output[K,V] {
    val outFormatClass : java.lang.Class[_ <: OutputFormat[K,V]]
    def setup(job : Job) : Unit;
  }
   
  trait IO[KWRITE, VWRITE, KREAD, VREAD] {
    val inFormatClass  : Class[_ <: InputFormat[KREAD, VREAD]];
    val outFormatClass : Class[_ <: OutputFormat[KWRITE,VWRITE]];
    val input:  Input[KREAD, VREAD];
    val output: Output[KWRITE,VWRITE];
  }

}

