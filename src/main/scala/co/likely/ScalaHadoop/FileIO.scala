package co.likely.ScalaHadoop

import org.apache.hadoop.mapreduce._;
import org.apache.hadoop.io._;
import org.apache.hadoop.fs.Path

import IO._

  /** This is a general class for inputs and outputs into the Map Reduce jobs.  Note that it's possible to
      write one type to a file and then have it be read as something else.  */

object FileIO {

  class FileInput[K, V] (
    val dirName: String,
    val inFormatClass : java.lang.Class[_ <: InputFormat[K,V]]
  ) extends Input[K,V] {
    override def setup(job : Job) : Unit = {
      lib.input.FileInputFormat.addInputPath(job, new Path(dirName));
    }
  }

  class FileOutput[K,V] (
    val dirName: String,
    val outFormatClass : java.lang.Class[_ <: lib.output.FileOutputFormat[K,V]]
  ) extends Output[K,V] {
    override def setup(job : Job) : Unit = {
      lib.output.FileOutputFormat.setOutputPath(job, new Path(dirName));
    }
  }

  class FileIO[KWRITE, VWRITE, KREAD, VREAD] (
    val dirName : String,
    val inFormatClass  : Class[_ <: InputFormat[KREAD, VREAD]],
    val outFormatClass : Class[lib.output.FileOutputFormat[KWRITE, VWRITE]] 
  ) extends IO[KWRITE, VWRITE, KREAD, VREAD] {
    val input: FileInput[KREAD, VREAD] = new FileInput(dirName, inFormatClass)
    val output: FileOutput[KWRITE, VWRITE] = new FileOutput(dirName, outFormatClass)
  }


  def SeqFile[K,V](dirName : String) (
    implicit mIn:   Manifest[lib.input.SequenceFileInputFormat[K,V]],
             mOut:  Manifest[lib.output.SequenceFileOutputFormat[K,V]]) = 
    new FileIO[K,V,K,V](dirName, 
      mIn .erasure.asInstanceOf[Class[lib.input.FileInputFormat[K,V]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K,V]]]);


  def Text[K,V](dirName : String)( 
    implicit mIn:   Manifest[lib.input.TextInputFormat],
             mOut:  Manifest[lib.output.TextOutputFormat[K,V]]) = 
    new FileIO[K,V,LongWritable,Text](dirName, 
      mIn .erasure.asInstanceOf[Class[lib.input.FileInputFormat[LongWritable,Text]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K,V]]]);

}
