package com.datamantra.producer


import java.io.File
import java.util.{Random, Properties}

import com.google.gson.{JsonObject, Gson}
import com.typesafe.config.ConfigFactory
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import java.nio.charset.Charset;
import org.apache.kafka.clients.producer._


/**
 * Created by kafka on 14/5/18.
 */
object TrasactionProducer {

  val applicationConf = ConfigFactory.load

  val props = new Properties()
  var topic:String =  _
  var producer:KafkaProducer[String, String] = _

  def loadCommonConf() = {

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.key.serializer"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.value.serializer"))
    props.put(ProducerConfig.ACKS_CONFIG, applicationConf.getString("kafka.acks"))
    props.put(ProducerConfig.RETRIES_CONFIG, applicationConf.getString("kafka.retries"))
    topic = applicationConf.getString("kafka.topic")
  }

  def loadLocalConf() = {
    loadCommonConf()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConf.getString("kafka.local.bootstrap.servers"))
  }

  def loadClusterConf() = {
    loadCommonConf()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConf.getString("kafka.cluster.bootstrap.servers"))
  }

  def getCsvIterator(fileName:String) = {

    val file = new File(fileName)
    val csvParser = CSVParser.parse(file, Charset.forName("UTF-8"), CSVFormat.DEFAULT)
    csvParser.iterator()
  }

/*
  def publishAvroRecord(fileName:String ): Unit = {

    val csvIterator = getCsvIterator(fileName)
    val rand: Random = new Random
    var count = 0
    while (csvIterator.hasNext) {

      val record = csvIterator.next()
      val transaction = new Transaction()
      println("Transaction Details:" + record.get(0),record.get(1),record.get(2),record.get(3),record.get(4),record.get(5),record.get(6),record.get(7),record.get(8),record.get(9), record.get(10), record.get(11))
      transaction.setCcNum(record.get(0))
      transaction.setFirst(record.get(1))
      transaction.setLast(record.get(2))
      transaction.setTransNum(record.get(3))
      transaction.setTransDate(record.get(4))
      transaction.setTransTime(record.get(5))
      transaction.setUnixTime(record.get(6))
      transaction.setCategory(record.get(7))
      transaction.setMerchant(record.get(8))
      transaction.setAmt(record.get(9).toDouble)
      transaction.setMerchLat(record.get(10))
      transaction.setMerchLong(record.get(11))
      val producerRecord = new ProducerRecord[String, Transaction](topic, transaction)
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)
      count = count + 1
    }
    println("record count: " + count)
  }
*/

  def publishJsonMsg(fileName:String) = {
    val gson: Gson = new Gson
    val csvIterator = getCsvIterator(fileName)
    val rand: Random = new Random
    var count = 0

    while (csvIterator.hasNext) {
      val record = csvIterator.next()
      println("Transaction Details:" + record.get(0),record.get(1),record.get(2),record.get(3),record.get(4),record.get(5),record.get(6),record.get(7),record.get(8),record.get(9), record.get(10), record.get(11))
      val obj: JsonObject = new JsonObject
      obj.addProperty(TransactionKafkaEnum.cc_num, record.get(0))
      obj.addProperty(TransactionKafkaEnum.first, record.get(1))
      obj.addProperty(TransactionKafkaEnum.last, record.get(2))
      obj.addProperty(TransactionKafkaEnum.trans_num, record.get(3))
      obj.addProperty(TransactionKafkaEnum.trans_date, record.get(4))
      obj.addProperty(TransactionKafkaEnum.trans_time, record.get(5))
      obj.addProperty(TransactionKafkaEnum.unix_time, record.get(6))
      obj.addProperty(TransactionKafkaEnum.category, record.get(7))
      obj.addProperty(TransactionKafkaEnum.merchant, record.get(8))
      obj.addProperty(TransactionKafkaEnum.amt, record.get(9))
      obj.addProperty(TransactionKafkaEnum.merch_lat, record.get(10))
      obj.addProperty(TransactionKafkaEnum.merch_long, record.get(11))
      val json: String = gson.toJson(obj)
      val producerRecord = new ProducerRecord[String, String](topic, json)
      producer.send(producerRecord, new MyProducerCallback)
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)
    }
  }

  class MyProducerCallback extends Callback {
    def onCompletion(recordMetadata: RecordMetadata, e: Exception) {
      if (e != null) System.out.println("AsynchronousProducer failed with an exception")
      else {
        System.out.println("Sent data to partition: " + recordMetadata.partition + " and offset: " + recordMetadata.offset)
      }
    }
  }

  def main(args: Array[String]) {

    val runMode = args(0)
    runMode match  {

      case "local" => loadLocalConf()
      case "cluster" => loadClusterConf()
      case _ => throw new IllegalArgumentException("Invalid Parameters")
    }
    producer = new KafkaProducer[String, String](props)
    val file = applicationConf.getString("kafka.producer.file")
    publishJsonMsg(file)

  }
}
