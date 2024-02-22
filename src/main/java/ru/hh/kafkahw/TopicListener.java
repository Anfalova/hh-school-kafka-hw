package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  // чтобы было не более одной обработки сообщения, будем проверять что счётчик в service ещё не увеличивался
  // таким образом, мы откинем повторные сообщения, которые возможно придут из sender
  // помечаем сообщение прочитанным независимо от результата обработки
  // в данном варианте решения существует вероятность, что сообщение, отправленное повторно,
  // начет обрабатываться дважды (если при обработке первой копии исключение вылетит до увеличения счётчика)
  // чтобы исправить эту ситуацию, можно было бы, например, изменить структуру сообщения
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    if (service.count("topic1", consumerRecord.value()) == 0) {
      try {
        service.handle("topic1", consumerRecord.value());
      } catch (Exception ignored) {
      }
    }
    ack.acknowledge();
  }

  // для данной семантики будем помечать сообщение только после того, как оно будет гарантировано обработано
  // для этого переместим ack.acknowledge(); в конец метода
  // если в handle вылетит исключение, Kafka его отловит и позже попробует повторно обработать данное сообщение
  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle("topic2", consumerRecord.value());
    ack.acknowledge();
  }

  // для exactly once объединим стратегии из двух предыдущих вариантов
  // в случае, если сообщение не было ни разу обработано, пытаемся это сделать, не отлавливая
  // исключения (как в случае с at least once)
  // есть вероятность, что при обработке сообщения после увеличения счётчика, вылезет исключение
  // то есть сообщение будет обработано не полностью. Данный код это не учитывает
  // чтобы обработать этот вариант, можно, например, переместить/добавить еще один счётчик в конец handle
  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    if (service.count("topic3", consumerRecord.value()) == 0) {
        service.handle("topic3", consumerRecord.value());
        ack.acknowledge();
    } else {
      ack.acknowledge();
    }
  }
}
