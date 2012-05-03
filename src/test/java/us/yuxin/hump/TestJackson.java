package us.yuxin.hump;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

public class TestJackson {
	@Test
	public void testObjectNode() throws IOException {
		String jsonStr = "[{\"id\":10, \"message\": \"hello world\"}]";

		ObjectMapper om = new ObjectMapper();
		JsonNode root = om.readValue(jsonStr, JsonNode.class);

		Assert.assertTrue(root.isArray());
		Assert.assertTrue(root.get(0).isObject());

		ObjectNode on = (ObjectNode)root.get(0);
		on.put("serial", 10);
		on.put("message", "Hello World");
		Assert.assertEquals("{\"id\":10,\"message\":\"Hello World\",\"serial\":10}", om.writeValueAsString(on));
	}
}
