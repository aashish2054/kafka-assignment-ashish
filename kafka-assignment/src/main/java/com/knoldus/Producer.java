package com.knoldus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;


public class Producer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.UserSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try{
            for(int i = 1; i <=80; i++){      //loop for producing the data

                Random ran = new Random();
                User userdata = new User(i, "Ashish Chaudhary", ran.nextInt(20)+20, "Btech");
                System.out.println("Message " + userdata.toString() + " sent!");
                kafkaProducer.send(new ProducerRecord("Assignment", Integer.toString(i), userdata));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}