package co.likely.ScalaHadoop;

trait OutTyped[KOUT, VOUT] {
  def kType : java.lang.Class[KOUT];
  def vType : java.lang.Class[VOUT];
}



