package com.visa.json.parser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

public class JSONParserTests {
	
	private JSONParser parser = new JSONParser();
	ContainerFactory factory = new ContainerFactory() {
		
		@SuppressWarnings("rawtypes")
		@Override
		public List<?> creatArrayContainer() {
			return new LinkedList();
		}
		
		@Override
		@SuppressWarnings("rawtypes")
		public Map createObjectContainer() {
			return new LinkedHashMap();
		}
	};
	
	public JSONParserTests() {
	}
	
	@SuppressWarnings({ "resource", "rawtypes" })
	@Test
	public void testJSONParser1() throws IOException, ParseException {
		// File file = new File("/tmp/Sample.json");
		InputStream fis;
		BufferedReader br;
		String line;
		//		StringBuffer jsonText = new StringBuffer();
		
		fis = new FileInputStream("/tmp/huge_stripped.json");
		br = new BufferedReader(new InputStreamReader(fis,
				Charset.forName("UTF-8")));
		while ((line = br.readLine()) != null) {
			if (line.equals("")) {
				continue;
			}
			//			jsonText.append(line);
			Map parsed = (Map) parser.parse(line, factory);
			for (Iterator iterator = parsed.entrySet().iterator(); iterator
					.hasNext();) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator
						.next();
				String val = entry.getValue().toString();
				String key = entry.getKey().toString();
				Assert.assertNotNull(val);
				Assert.assertTrue(key != null && key.length() > 0);
				System.out.println(key + ">>> " + val);
				//				jsonText = new StringBuffer();
			}
			
		}
		br.close();
		br = null;
		fis = null;
		
		// Done with the file
	}
}
