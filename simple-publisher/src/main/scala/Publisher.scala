import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import scodec.codecs._

import scala.util.{Failure, Random, Success, Try}

/**
  * Created by snipy
  * Project simple-publisher
  */
object Publisher extends App {
  // Global information
  val name = "apdl-case-study-from-scratch"

  // Codec information
  // We want to transfer temp and pressure
  val codec = int32 ~ int32

  // MQTT Server info to publish
  val brokerURL = "tcp://mosquitto-mqtt:1883"
  val topic = s"$name-topic"
  val msg = "Hello"
  val persistence = new MqttDefaultFilePersistence("tmp")
  Try {
    val client = new MqttClient(brokerURL, MqttClient.generateClientId(), persistence)
    client.connect()

    val msgTopic = client.getTopic(topic)
    val message = new MqttMessage(msg.getBytes("utf-8"))

    while (true) {
      val temp = Random.nextInt(20)
      val pressure = Random.nextInt(50) + 50
      val encode = codec.encode((temp, pressure))
      val data = Try {
        encode.map(_.toByteArray).require
      } match {
        case Failure(exception) =>
          println(s"Can't encode properly : $exception")
          throw new UnsupportedOperationException
        case Success(value) => value
      }

      val message = new MqttMessage(data)

      println(s"Publish $temp° and $pressure P at ${System.currentTimeMillis()}")
      msgTopic.publish(message)
      Thread.sleep(1000)
    }
    while(true) {
      println("OK !")
      Thread.sleep(1000)
    }

  } match {
    case Failure(exception) => println(s"ERROR : $exception + ${exception.getCause}")
    case Success(_) => println(s"OK !")
  }
}
