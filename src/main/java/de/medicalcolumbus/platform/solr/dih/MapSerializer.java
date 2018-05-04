package de.medicalcolumbus.platform.solr.dih;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class MapSerializer implements Serializer<Map<String, Object>>, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(MapSerializer.class);

	@Override
	public void serialize(@NotNull DataOutput2 out, @NotNull Map<String, Object> value) throws IOException {
		ObjectOutput objectOutput = new ObjectOutputStream(out);
		objectOutput.writeObject(value);
	}

	@Override
	public Map<String, Object> deserialize(@NotNull DataInput2 input, int available) throws IOException {
//
//		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(input.internalByteArray());
//		ObjectInput objectInput = new ObjectInputStream(byteArrayInputStream);
//		ObjectMapper objectMapper = new ObjectMapper();
//
//		Map<String, Object>  returnMap;
//
//		try {
//			Object map = objectInput.readObject();
//
//			returnMap = objectMapper.convertValue(map, Map.class);
//
//		} catch (ClassNotFoundException e) {
//			LOG.error(e.getMessage());
//			throw new RuntimeException(e.getMessage());
//		}
//
//		return returnMap;

		throw new UnsupportedOperationException();
	}
}
