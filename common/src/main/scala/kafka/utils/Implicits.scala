package kafka.utils

import java.util
import java.util.Properties

import scala.jdk.CollectionConverters._

object Implicits {

    implicit class PropertiesOps(properties: Properties) {

        def ++=(props: Properties): Unit =
            (properties: util.Hashtable[AnyRef, AnyRef]).putAll(props)
        
        def ++=(map: collection.Map[String, AnyRef]): Unit =
            (properties: util.Hashtable[AnyRef, AnyRef]).putAll(map.asJava)
    }
}
