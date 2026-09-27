package kafka

import java.util.Properties
import joptsimple.OptionParser
import kafka.server.{KafkaConfig, KafkaRaftServer, Server}
import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.common.utils.internals.{Exit, Java, LoggingSignalHandler, OperatingSystem}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.server.util.CommandLineUtils

object Kafka extends Logging {

    def getPropsFromArgs(args: Array[String]): Properties = {
        val optionParser = new OptionParser(false)
        val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file (e.g. Key=value)")
            .withRequiredArg()
            .ofType(classOf[String])
        
        optionParser.accepts("version", "Print version information and exit.")

        if (args.isEmpty || args.contains("--help")) {
            CommandLineUtils.printUsageAndExit(optionParser,
                "USAGE: java [options] %s server.properties [--override property=value]*".format(this.getClass.getCanonicalName.split('$').head))
        }

        if (args.contains("--version")) {
            CommandLineUtils.printVersionAndExit()
        }

        val props = Utils.loadProps(args(0))

        if (args.length > 1) {
            val options = optionParser.parse(args.slice(1, args.length): _*)

            if (options.nonOptionArguments().size() > 0) {
                CommandLineUtils.printUsageAndExit(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
            }

            props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt))
        }
        props
    }

    private def buildServer(props: Properties): Server = {
        val config = KafkaConfig.fromProps(props, doLog = false)
        new KafkaRaftServer(
        config,
        Time.SYSTEM,
        )
    }

    def main(args: Array[String]): Unit = {
        try {
            val serverProps = getPropsFromArgs(args)
            val server = buildServer(serverProps)

            try {
                if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
                    new LoggingSignalHandler().register()
            } catch {
                case e: ReflectiveOperationException => 
                    warn("Failed to register optional signal handler that logs a message when the process is terminated " +
                s"by a signal. Reason for registration failure is: $e", e)
            }

            Exit.addShutdownHook("kafka-shutdown-hook", () => {
                try server.shutdown()
                catch {
                    case _: Throwable => 
                        fatal("Halting Kafka.")
                        Exit.halt(1)
                }
            })

            try server.startup()
            catch {
                case e: Throwable =>
                    fatal("Exiting Kafka due to fatal exception during startup.", e)
                    Exit.exit(1)
            }

            server.awaitShutdown()
        }
        catch {
            case e: Throwable =>
                fatal("Exiting Kafka due to fatal exception", e)
                Exit.exit(1)
        }
        Exit.exit(0)
    }
}
