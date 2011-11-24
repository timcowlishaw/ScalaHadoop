package co.likely.ScalaHadoop

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.client._
import scala.reflect.Manifest
import IO._

object HBaseIO {
  class HBaseInput(
    val inFormatClass : java.lang.Class[TableInputFormat]
  ) extends Input[ImmutableBytesWritable, Result] {
    override def setup(job : Job) : Unit = ();
  }

  class HBaseSingleTableOutput[V <: Writable] (
    val outFormatClass : java.lang.Class[TableOutputFormat[NullWritable]]
  ) extends Output[NullWritable, V] {
    override def setup(job : Job) : Unit = ();
  }

  class HBaseMultiTableOutput[V <: Writable] (
    val outFormatClass : java.lang.Class[MultiTableOutputFormat]
  ) extends Output[ImmutableBytesWritable, V] {
    override def setup(job : Job) : Unit = ();
  }

  class HBaseMultiTableIO[V <: Writable] extends IO[ImmutableBytesWritable, V, ImmutableBytesWritable, Result] {
    val inFormatClass  : Class[TableInputFormat] = classOf[TableInputFormat];
    val outFormatClass : Class[MultiTableOutputFormat] = classOf[MultiTableOutputFormat];
    val input = new HBaseInput(inFormatClass);
    val output = new HBaseMultiTableOutput[V](outFormatClass); 
  }

  class HBaseSingleTableIO[V <: Writable] extends IO[NullWritable, V, ImmutableBytesWritable, Result] {
    val inFormatClass : Class[TableInputFormat] = classOf[TableInputFormat];
    val outFormatClass : Class[TableOutputFormat[NullWritable]] = classOf[TableOutputFormat[NullWritable]];
    val input = new HBaseInput(inFormatClass);
    val output = new HBaseSingleTableOutput[V](outFormatClass);
  }

  def MultiTable[V <: Writable] = new HBaseMultiTableIO[V];
  def SingleTable[V <: Writable] = new HBaseSingleTableIO[V];
}
