package xmltokafka.xml

import java.util.Date

/**
  * Created by root on 8/8/17.
  */
object XMLRandomFunct {

  private val rnd = new scala.util.Random

  def randomString(names:Array[String]): String ={
    val random_index = rnd.nextInt(names.length)
    names(random_index)
  }

  def randomInteger(i:Integer, j:Integer): Integer ={
    i + rnd.nextInt( (j - i) + 1 )
  }

  def randomDouble(i:Double, j:Double): Double={
    i + (j - i) * rnd.nextDouble();
  }

  def randomBoolean(): Boolean = {
    rnd.nextBoolean()
  }
}
