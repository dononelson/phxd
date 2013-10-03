package com.visa.json.parser;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JacksonParserTest {

	public JacksonParserTest() {
	}

	@Test
	public void testJacksonParserCreation() {
		JsonFactory jsonF = new JsonFactory();
		try {
			JsonParser jp = jsonF.createParser(new File("/tmp/sample.json"));
			if (jp.nextToken() != JsonToken.START_OBJECT) {
				throw new IOException("Expected data to start with an Object");
			}
			while (jp.nextToken() != JsonToken.END_OBJECT) {
				String fieldName = jp.getCurrentName();
				// Let's move to value
				jp.nextToken();
				String value = jp.getValueAsString();
				System.out.println(fieldName + ": " + value);
			}
			jp.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
