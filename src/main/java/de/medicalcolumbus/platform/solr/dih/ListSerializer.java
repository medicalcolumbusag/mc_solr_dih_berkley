package de.medicalcolumbus.platform.solr.dih;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializerObjectArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ListSerializer<T> extends GroupSerializerObjectArray<List<T>> implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(MapSerializer.class);

	private Serializer<T> listValueSerializer;

	public ListSerializer(Serializer<T> listValueSerializer) {
		this.listValueSerializer = listValueSerializer;
	}

	@Override
	public void serialize(@NotNull DataOutput2 out, @NotNull List<T> value) throws IOException {
		for (T v : value) {
			listValueSerializer.serialize(out, v);
		}
	}

	@Override
	public List<T> deserialize(@NotNull DataInput2 input, int available) throws IOException {
		List<T> mapList = new ArrayList<>();

		while (available != -1 && available != 0) {
			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(input.internalByteArray());
			ObjectInput objectInput = new ObjectInputStream(byteArrayInputStream);
			ObjectMapper objectMapper = new ObjectMapper();


			try {
				Object map = objectInput.readObject();

				mapList = objectMapper.convertValue(map, new TypeReference<List<T>>(){});

			} catch (ClassNotFoundException e) {
				LOG.error(e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
		}

		return mapList;
	}


}
