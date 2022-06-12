package syn

import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import java.util.Random
import scala.collection.mutable

// The functions in this class have been adapted from Spark Sampling utils.
object SamplingUtils {
  // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/core/src/main/scala/org/apache/spark/util/random/StratifiedSamplingUtils.scala#L282
  class RandomDataGenerator {
    // commons-math3 doesn't have a method to generate Poisson from an arbitrary mean;
    // maintain a cache of Poisson(m) distributions for various m
    val poissonCache: mutable.Map[Double, PoissonDistribution] = mutable.Map[Double, PoissonDistribution]()
    var poissonSeed = 0L

    def reSeed(seed: Long): Unit = {
      poissonSeed = seed
      poissonCache.clear()
    }

    def nextPoisson(mean: Double): Int = {
      val poisson = poissonCache.getOrElseUpdate(mean, {
        val newPoisson = new PoissonDistribution(mean)
        newPoisson.reseedRandomGenerator(poissonSeed)
        newPoisson
      })
      poisson.sample()
    }
  }
  // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/core/src/main/scala/org/apache/spark/util/random/StratifiedSamplingUtils.scala#L237
  def getSamplingFunc(samplingFraction: Double): (Int, Iterator[(String, (GenericRowWithSchema, GenericRowWithSchema))]) => Iterator[(String, GenericRowWithSchema, Int)] = {
    val seed = new Random().nextLong
    (idx: Int, iter: Iterator[(String, (GenericRowWithSchema, GenericRowWithSchema))]) => {
      val rng = new RandomDataGenerator()
      rng.reSeed(seed + idx)
      iter.flatMap { item =>
        val count = rng.nextPoisson(item._2._2.get(1).asInstanceOf[Long] * samplingFraction)
        if (count == 0) {
          Iterator.empty
        } else {
          Iterator.fill(1)((item._1, item._2._1, count))
        }
      }
    }
  }
}
