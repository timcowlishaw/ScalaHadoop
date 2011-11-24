package co.likely.ScalaHadoop;
import org.apache.hadoop.mapreduce._;

abstract class MapReduceTask[KIN, VIN, KOUT, VOUT]   {

  // TODO: Should this be Writable?
  protected var mapper   : TypedMapper[KIN, VIN, _, _] = null ;
  protected var reducer  : TypedReducer[_, _, KOUT, VOUT]   = null;

  var name = "NO NAME"; 

  def initJob(job: Job) = {
    job setMapperClass         mapper.getClass.asInstanceOf[java.lang.Class[ Mapper[_,_,_,_]]];
    job setMapOutputKeyClass   mapper.kType;
    job setMapOutputValueClass mapper.vType;
    if(reducer != null) {
        job setReducerClass       reducer.getClass.asInstanceOf[java.lang.Class[ Reducer[_,_,_,_]]];
        job setOutputKeyClass     reducer.kType;
        job setOutputValueClass   reducer.vType;
      }
      else {
        job setOutputKeyClass     mapper.kType;
        job setOutputValueClass   mapper.vType;
      }
    }

}

object MapReduceTask {

  // KINT and VINT are the key/value types of the intermediate steps
    implicit def fromMapper[KIN, VIN, KOUT, VOUT](mapper: TypedMapper[KIN, VIN, KOUT, VOUT]) : MapOnlyTask[KIN,VIN,KOUT,VOUT] = {
      val mapReduceTask= new MapOnlyTask[KIN, VIN, KOUT, VOUT]();
      mapReduceTask.mapper = mapper;
      return mapReduceTask;
    }


    def MapReduceTask[KIN, VIN, KOUT, VOUT, KTMP, VTMP]
         (mapper: TypedMapper[KIN, VIN, KTMP, VTMP], reducer: TypedReducer[KTMP, VTMP, KOUT, VOUT], name: String)
         : MapAndReduceTask[KIN, VIN, KOUT, VOUT] = {
           val mapReduceTask= new MapAndReduceTask[KIN, VIN, KOUT, VOUT]();
           mapReduceTask.mapper  = mapper;
           mapReduceTask.reducer = reducer;
           mapReduceTask.name    = name;
           return mapReduceTask;
         }

    def MapReduceTask[KIN, VIN, KOUT, VOUT, KTMP, VTMP]
         (mapper: TypedMapper[KIN, VIN, KTMP, VTMP], reducer: TypedReducer[KTMP, VTMP, KOUT, VOUT])
         : MapAndReduceTask[KIN, VIN, KOUT, VOUT] = MapReduceTask(mapper, reducer, "NO NAME");

}

class MapOnlyTask[KIN, VIN, KOUT, VOUT] 
      extends MapReduceTask[KIN, VIN, KOUT, VOUT]    { }

class MapAndReduceTask[KIN, VIN, KOUT, VOUT] 
      extends MapReduceTask[KIN, VIN, KOUT, VOUT]    { }



