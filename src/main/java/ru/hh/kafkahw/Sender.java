package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }

  // добавила цикл, чтобы гарантировать отправку сообщения
  // сообщение может быть отправлено более одного раза, но точно уйдет хотя бы один раз
  public void doSomething(String topic, String message) {
    boolean flag = false;
    while (!flag) {
      try {
        producer.send(topic, message);
        flag = true;
      } catch (Exception ignore) {
      }
    }
  }
}
