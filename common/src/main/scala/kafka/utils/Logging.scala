package kafka.utils

import com.typesafe.scalalogging.Logger
import io.github.kafkaprinciple.common.logger.Log4jControllerRegistration
import org.slf4j.{LoggerFactory, Marker, MarkerFactory}

private object Logging {
    private val FatalMarker: Marker = MarkerFactory.getMarker("FATAL")
}

trait Logging {

    protected lazy val logger: Logger = Logger(LoggerFactory.getLogger(loggerName))

    protected var logIdent: String = _

    Log4jControllerRegistration.register()

    protected def loggerName: String = getClass.getName

    protected def msgWithLogIdent(msg: String): String =
        if (logIdent == null) msg else logIdent + msg
    
    def trace(msg: => String): Unit = logger.trace(msgWithLogIdent(msg))

    def trace(msg: => String, e: => Throwable): Unit = logger.trace(msgWithLogIdent(msg),e)

    def isDebugEnabled: Boolean = logger.underlying.isDebugEnabled

    def isTraceEnabled: Boolean = logger.underlying.isTraceEnabled

    def debug(msg: => String): Unit = logger.debug(msgWithLogIdent(msg))

    def debug(msg: => String, e: => Throwable): Unit = logger.debug(msgWithLogIdent(msg),e)

    def info(msg: => String): Unit = logger.info(msgWithLogIdent(msg))

    def info(msg: => String,e: => Throwable): Unit = logger.info(msgWithLogIdent(msg),e)

    def warn(msg: => String): Unit = logger.warn(msgWithLogIdent(msg))

    def warn(msg: => String, e: => Throwable): Unit = logger.warn(msgWithLogIdent(msg),e)

    def error(msg: => String): Unit = logger.error(msgWithLogIdent(msg))

    def error(msg: => String, e: => Throwable): Unit = logger.error(msgWithLogIdent(msg),e)

    def fatal(msg: => String): Unit = 
        logger.error(Logging.FatalMarker, msgWithLogIdent(msg))

    def fatal(msg: => String, e: => Throwable): Unit = 
        logger.error(Logging.FatalMarker, msgWithLogIdent(msg), e)
}
