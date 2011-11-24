package co.likely.ScalaHadoop;
import org.apache.hadoop.mapreduce._;
import scala.reflect.Manifest;

abstract class TypedMapper[KIN, VIN, KOUT, VOUT] (implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT]) 
         extends Mapper[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] 
  { 
    def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]];
    def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]];
    type ContextType =  Mapper[KIN, VIN, KOUT, VOUT]#Context;

import scala.reflect.Manifest;
    var k: KIN = _;
    var v: VIN = _;
    var context : ContextType = _;

    override def map(k: KIN, v:VIN, context: ContextType) : Unit =  { 
      this.k = k;
      this.v = v;
      this.context = context;
      doMap;
    }

    def doMap : Unit = {}

  } 
