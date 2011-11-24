package co.likely.ScalaHadoop

import org.apache.hadoop.mapreduce._;
import org.apache.hadoop.io._;

object IO {

  trait Input[K, V] {
     val inFormatClass : java.lang.Class[_ <: InputFormat[_ <: K, _ <: V]]
     def setup(job : Job) : Unit;
  }

  trait Output[K,V] {
    val outFormatClass : java.lang.Class[_ <: OutputFormat[_ >: K, _ >: V]]
    def setup(job : Job) : Unit;
  }
   
  trait IO[KWRITE, VWRITE, KREAD, VREAD] {
    val inFormatClass  : Class[_ <: InputFormat[_ <: KREAD,_ <:  VREAD]];
    val outFormatClass : Class[_ <: OutputFormat[_ >: KWRITE, _ >: VWRITE]];
    val input:  Input[KREAD, VREAD];
    val output: Output[KWRITE,VWRITE];
  }

}

