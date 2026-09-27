package kafka.utils

import java.util.Properties
import java.util.Collections
import scala.collection._
import scala.jdk.CollectionConverters._
import kafka.utils.Implicits._
import org.apache.kafka.server.util.Csv

object VerifiableProperties {
    def apply(map: java.util.Map[String, AnyRef]): VerifiableProperties = {
        val props = new Properties()
        props ++= map.asScala
        new VerifiableProperties(props)
    }
}

class VerifiableProperties(val props: Properties) extends Logging {
    private val referenceSet = mutable.HashSet[String]()

    def this() = this(new Properties)

    def containsKey(name: String): Boolean = {
        props.containsKey(name)
    }

    def getProperty(name: String): String = {
        val value = props.getProperty(name)
        referenceSet.add(name)
        if (value == null) value else value.trim()
    }

    def getInt(name: String): Int = getString(name).toInt

    def getInt(name: String, default: Int): Int =
        getIntInRange(name, default, (Int.MinValue, Int.MaxValue))

    def getShort(name: String, default: Short): Short =
        getShortInRange(name, default, (Short.MinValue, Short.MaxValue))

    private def getIntInRange(name: String, default: Int, range: (Int, Int)): Int = {
        val v =
            if (containsKey(name))
                getProperty(name).toInt
            else
                default
        require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
    }

    private def getShortInRange(name: String, default: Short, range: (Short, Short)): Short = {
        val v =
            if (containsKey(name))
                getProperty(name).toShort
            else
                default
            require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
            v
    }

    def getLong(name: String): Long = getString(name).toLong

    def getLong(name: String, default: Long): Long = 
        getLongInRange(name, default, (Long.MinValue, Long.MaxValue))
    
    private def getLongInRange(name: String, default: Long, range: (Long, Long)): Long = {
        val v =
            if (containsKey(name))
                getProperty(name).toLong
            else
                default
        require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
        v
    }

    def getDouble(name: String): Double = getString(name).toDouble

    def getDouble(name: String, default: Double): Double = {
        if (containsKey(name))
            getDouble(name)
        else
            default
    }

    def getBoolean(name: String, default: Boolean): Boolean = {
        if (!containsKey(name))
            default
        else {
            val v = getProperty(name)
            require(v == "true" || v == "false", "Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false")
            v.toBoolean
        }
    }

    def getBoolean(name: String): Boolean = getString(name).toBoolean

    def getString(name: String, default: String): String = {
        if (containsKey(name))
            getProperty(name)
        else
            default
    }


    def getString(name: String): String = {
        require(containsKey(name), "Missing required property '" + name + "'")
        getProperty(name)
    }

    def getMap(name: String, valid: String => Boolean = _ => true): Map[String, String] = {
        try {
            val m = Csv.parseCsvMap(getString(name, "")).asScala
            m.foreach {
                case (key, value) => 
                    if(!valid(value))
                        throw new IllegalArgumentException("Invalid entry '%s' = '%s' for property '%s'".format(key, value, name))
            }
            m
        } catch {
            case e : Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(name, e.getMessage))
        }
    }

    def verify(): Unit = {
        info("Verifying properties")
        val propNames = Collections.list(props.propertyNames).asScala.map(_.toString()).sorted
        for (key <- propNames) {
            if (!referenceSet.contains(key) && !key.startsWith("external"))
                warn("Property %s is not valid".format(key))
            else
                info("Property %s is overridden to %s".format(key, props.getProperty(key)))
        }
    }

    override def toString: String = props.toString
}
