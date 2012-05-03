package us.yuxin.hump;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Lists;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class HumpFeeder implements Runnable {
	private int parallel;

	private ObjectMapper mapper;
	private File[] sources;

	private BlockingQueue<String> taskQueue;
	private BlockingQueue<String> feedbackQueue;

	public void setup(File[] jsonSources,
										BlockingQueue<String> taskQueue,
										BlockingQueue<String> feedbackQueue,
										int parallel) {
		this.sources = jsonSources;
		this.taskQueue = taskQueue;
		this.feedbackQueue = feedbackQueue;
		this.parallel = parallel;
	}

	public void setup(File jsonSource,
										BlockingQueue<String> taskQueue, BlockingQueue<String> feedbackQueue,
										int parallel) throws IOException {
		this.sources = new File[1];
		this.sources[0] = jsonSource;
		this.taskQueue = taskQueue;
		this.feedbackQueue = feedbackQueue;
		this.parallel = parallel;
	}

	public void run() {
		int serial = 0;
		mapper = new ObjectMapper();

		for (File source : sources) {
			try {
				JsonNode root = mapper.readValue(source, JsonNode.class);
				List<JsonNode> tasks = Lists.newArrayList(root);
				Collections.shuffle(tasks);

				for (JsonNode node : tasks) {

					if (node.isObject()) {
						ObjectNode on = (ObjectNode) node;
						on.put("serial", serial);
						taskQueue.offer(mapper.writeValueAsString(on));
					} else {
						taskQueue.offer(mapper.writeValueAsString(node));
					}
					++serial;
				}
			} catch (IOException e) {
				e.printStackTrace();
				// TODO Exception handler.
			}
		}

		for (int i = 0; i < parallel; ++i) {
			taskQueue.offer("STOP");
		}
	}
}
