package fuchs.ad_producer;

import fuchs.ad_producer.model.ClickEvent;
import fuchs.ad_producer.model.ClickEventFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@ComponentScan(basePackages = "fuchs.ad_producer")
public class AdProducerApplication {

	private static KafkaTemplate<String, byte[]> kafkaTemplate;
	private static final String TOPIC = "click_events";
	private static Schema schema;
	private final String SCHEMA_PATH = "src/main/resources/avro/click_event.avsc";
	public AdProducerApplication(KafkaTemplate<String, byte[]> kafkaTemplate ) {
		AdProducerApplication.kafkaTemplate = kafkaTemplate;
		try{
			schema = new Schema.Parser().parse(new File(SCHEMA_PATH));
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(AdProducerApplication.class, args);
		produceMessages(1000,1,true);
	}

	public static void produceMessages(int messagesNum, int period, boolean log){
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

		Runnable task = () -> {

			for (int i = 0; i < messagesNum; i++) {
				ClickEvent randomEvent = ClickEventFactory.generateRandomClickEvent();

				try {
					GenericRecord record = new GenericData.Record(schema);
					record.put("event_id", randomEvent.getEventId());
					record.put("timestamp", randomEvent.getTimestamp());
					record.put("ad_id", randomEvent.getAdId());
					record.put("location", randomEvent.getLocation());
					record.put("category", randomEvent.getCategory());
					record.put("platform", randomEvent.getPlatform());


					ByteArrayOutputStream out = new ByteArrayOutputStream();
					DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
					Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
					writer.write(record, encoder);
					encoder.flush();
					out.close();

					byte[] bytes = out.toByteArray();

					kafkaTemplate.send(TOPIC, randomEvent.getPlatform()+"-"+randomEvent.getCategory(), bytes);
				}catch (Exception e){
					e.printStackTrace();
				}
			}

			if (log) System.out.println("Messages Sent");
		};

		scheduler.scheduleAtFixedRate(task, 1, period, TimeUnit.SECONDS);
	}


}
