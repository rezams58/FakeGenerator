package fakegen;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class IsolationTest {
 @Test void sharedThreadBlocksHealthyPartitionDuringLocalRetry() throws Exception { probe(1); }
 @Test void separateThreadsAllowHealthyPartitionDuringLocalRetry() throws Exception { probe(2); }

 private void probe(int threads) throws Exception {
  String broker=System.getenv("FAKEGEN_TEST_KAFKA");
  assertNotNull(broker,"FAKEGEN_TEST_KAFKA required; integration test not optional");
  String id="fakegen-isolation-"+UUID.randomUUID(), input=id+"-in", output=id+"-out";
  var blocked=new CountDownLatch(1); var release=new CountDownLatch(1);
  Map<String,String> processingThreads=new ConcurrentHashMap<>();
  Properties props=new Properties(); props.put("bootstrap.servers",broker);
  try(Admin admin=Admin.create(props)) {
   admin.createTopics(List.of(new NewTopic(input,2,(short)1),new NewTopic(output,2,(short)1))).all().get(30,TimeUnit.SECONDS);
   try {
    props.put("application.id",id); props.put("num.stream.threads",threads);
    props.put("processing.guarantee","exactly_once_v2"); props.put("commit.interval.ms",100);
    props.put("default.key.serde",Serdes.StringSerde.class.getName()); props.put("default.value.serde",Serdes.StringSerde.class.getName());
    props.put("consumer.auto.offset.reset","earliest"); props.put("consumer.max.poll.interval.ms",60000);
    StreamsBuilder builder=new StreamsBuilder();
    builder.<String,String>stream(input).peek((k,v)->{
     processingThreads.put(v,Thread.currentThread().getName());
     if(v.equals("blocked")) {
      blocked.countDown();
      try { if(!release.await(30,TimeUnit.SECONDS)) throw new IllegalStateException("probe release timeout"); }
      catch(InterruptedException e){Thread.currentThread().interrupt(); throw new RuntimeException(e);}
     }
    }).to(output);
    Properties pp=new Properties(); pp.put("bootstrap.servers",broker); pp.put("key.serializer",StringSerializer.class.getName()); pp.put("value.serializer",StringSerializer.class.getName()); pp.put("acks","all");
    Properties cp=new Properties(); cp.put("bootstrap.servers",broker); cp.put("group.id",id+"-audit"); cp.put("key.deserializer",StringDeserializer.class.getName()); cp.put("value.deserializer",StringDeserializer.class.getName()); cp.put("isolation.level","read_committed"); cp.put("enable.auto.commit",false); cp.put("auto.offset.reset","earliest");
    try(KafkaStreams streams=new KafkaStreams(builder.build(),props); KafkaProducer<String,String> producer=new KafkaProducer<>(pp); KafkaConsumer<String,String> consumer=new KafkaConsumer<>(cp)) {
     try {
      consumer.assign(List.of(new TopicPartition(output,0),new TopicPartition(output,1))); consumer.seekToBeginning(consumer.assignment()); streams.start();
      producer.send(new ProducerRecord<>(input,0,"a","warm0")).get(30,TimeUnit.SECONDS);
      producer.send(new ProducerRecord<>(input,1,"b","warm1")).get(30,TimeUnit.SECONDS);
      List<String> warm=new ArrayList<>(); long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
      while(warm.size()<2 && System.nanoTime()<until) collect(consumer,warm);
      assertEquals(Map.of("warm0",1L,"warm1",1L),counts(warm));
      if(threads==2) assertNotEquals(processingThreads.get("warm0"),processingThreads.get("warm1"),"tasks must actually occupy different threads");
      else assertEquals(processingThreads.get("warm0"),processingThreads.get("warm1"));
      producer.send(new ProducerRecord<>(input,0,"a","blocked")).get(30,TimeUnit.SECONDS);
      assertTrue(blocked.await(30,TimeUnit.SECONDS),"blocking task did not start");
      producer.send(new ProducerRecord<>(input,0,"a","follower")).get(30,TimeUnit.SECONDS);
      producer.send(new ProducerRecord<>(input,1,"b","healthy")).get(30,TimeUnit.SECONDS);
      List<String> observed=new ArrayList<>(); until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
      while(System.nanoTime()<until) collect(consumer,observed);
      var offsets=admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata().get(20,TimeUnit.SECONDS);
      assertEquals(1L,offsets.get(new TopicPartition(input,0)).offset(),"blocked input offset must remain at warmup");
      assertEquals(threads==2 ? 2L : 1L,offsets.get(new TopicPartition(input,1)).offset());
      assertEquals(threads==2 ? Map.of("healthy",1L) : Map.of(),counts(observed));
      assertFalse(processingThreads.containsKey("follower"),"follower overtook blocked record");
      System.out.println("EVIDENCE threads="+threads+" phase=blocked committed="+counts(observed)+" offsets="+offsets+" processingThreads="+processingThreads);
      release.countDown(); until=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
      while(observed.size()<3 && System.nanoTime()<until) collect(consumer,observed);
      // Continue reading after all expected records to detect additional committed copies.
      until=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
      while(System.nanoTime()<until) collect(consumer,observed);
      assertEquals(Map.of("blocked",1L,"follower",1L,"healthy",1L),counts(observed));
      assertTrue(observed.indexOf("blocked")<observed.indexOf("follower"));
      offsets=admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata().get(20,TimeUnit.SECONDS);
      assertEquals(3L,offsets.get(new TopicPartition(input,0)).offset());
      assertEquals(2L,offsets.get(new TopicPartition(input,1)).offset());
      System.out.println("EVIDENCE threads="+threads+" phase=released exactCounts="+counts(observed)+" offsets="+offsets+" processingThreads="+processingThreads);
     } finally { release.countDown(); }
    }
   } finally { admin.deleteTopics(List.of(input,output)).all().get(30,TimeUnit.SECONDS); }
  }
 }
 private static void collect(KafkaConsumer<String,String> consumer,List<String> values) {
  for(var r:consumer.poll(Duration.ofMillis(200))) values.add(r.value());
 }
 private static Map<String,Long> counts(List<String> values) {
  Map<String,Long> counts=new TreeMap<>(); values.forEach(v->counts.merge(v,1L,Long::sum)); return counts;
 }
}
