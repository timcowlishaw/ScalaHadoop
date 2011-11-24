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

  class HBaseSingleTableOutput (
    val outFormatClass : java.lang.Class[TableOutputFormat[NullWritable]]
  ) extends Output[NullWritable, Writable] {
    override def setup(job : Job) : Unit = ();
  }

  class HBaseMultiTableOutput (
    val outFormatClass : java.lang.Class[MultiTableOutputFormat]
  ) extends Output[ImmutableBytesWritable, Writable] {
    override def setup(job : Job) : Unit = ();
  }

  class HBaseMultiTableIO extends IO[ImmutableBytesWritable, Writable, ImmutableBytesWritable, Result] {
    val inFormatClass  : Class[TableInputFormat] = classOf[TableInputFormat];
    val outFormatClass : Class[MultiTableOutputFormat] = classOf[MultiTableOutputFormat];
    val input = new HBaseInput(inFormatClass);
    val output = new HBaseMultiTableOutput(outFormatClass); 
  }

  class HBaseSingleTableIO extends IO[NullWritable, Writable, ImmutableBytesWritable, Result] {
    val inFormatClass : Class[TableInputFormat] = classOf[TableInputFormat];
    val outFormatClass : Class[TableOutputFormat[NullWritable]] = classOf[TableOutputFormat[NullWritable]];
    val input = new HBaseInput(inFormatClass);
    val output = new HBaseSingleTableOutput(outFormatClass);
  }

  def MultiTable = new HBaseMultiTableIO;
  def SingleTable = new HBaseSingleTableIO;
}
