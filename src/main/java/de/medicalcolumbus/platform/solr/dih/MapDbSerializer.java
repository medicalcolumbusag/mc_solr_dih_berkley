package de.medicalcolumbus.platform.solr.dih;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.isNull;

public class MapDbSerializer implements Serializer<HashMap<String, Object>>, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(MapDbSerializer.class);

	@Override
	public void serialize(@NotNull DataOutput2 out, @NotNull HashMap<String, Object> value) throws IOException {
		for (Map.Entry<String, Object> entry : value.entrySet()) {

			Object o = entry.getValue();

			if (!isNull(o)) {
				//serialize entry key
				out.writeUTF(entry.getKey());

				// serialize key based on type
				// first value written is the type so we know what to read during deserialization
				// second value is the actual value

				if (o instanceof Byte) {
					out.writeUTF(DIHCacheTypes.BYTE.name());
					out.writeByte((byte) o);
				} else if (o instanceof Short) {
					out.writeUTF(DIHCacheTypes.SHORT.name());
					out.writeShort((short) o);
				} else if (o instanceof Integer) {
					out.writeUTF(DIHCacheTypes.INTEGER.name());
					out.writeInt((int) o);
				} else if (o instanceof Float) {
					out.writeUTF(DIHCacheTypes.FLOAT.name());
					out.writeFloat((float) o);
				} else if (o instanceof Double) {
					out.writeUTF(DIHCacheTypes.DOUBLE.name());
					out.writeDouble((double) o);
				} else if (o instanceof Boolean) {
					out.writeUTF(DIHCacheTypes.BOOLEAN.name());
					out.writeBoolean((boolean) o);
				} else if (o instanceof Character) {
					out.writeUTF(DIHCacheTypes.CHARACTER.name());
					out.writeChar((char) o);
				} else if (o instanceof Long) {
					out.writeUTF(DIHCacheTypes.LONG.name());
					out.writeLong((long) o);
				} else {
					out.writeUTF(DIHCacheTypes.STRING.name());
					out.writeUTF((String) o);
				}
			}
		}
	}

	@Override
	public HashMap<String, Object> deserialize(@NotNull DataInput2 input, int available) throws IOException {

		HashMap<String, Object>  returnMap = new HashMap<>();

		String key = input.readUTF();

		String type = input.readUTF();

		Object value = deserializeValue(input, type);

		returnMap.put(key, value);

		return returnMap;

	}

	private Object deserializeValue(DataInput2 input, String type) {
		try {
			if (type.equalsIgnoreCase(DIHCacheTypes.BYTE.name())) {
				return input.readByte();
			} else if (type.equalsIgnoreCase(DIHCacheTypes.SHORT.name())) {
				return input.readShort();
			} else if (type.equalsIgnoreCase(DIHCacheTypes.INTEGER.name())) {
				return input.readInt();
			} else if (type.equalsIgnoreCase(DIHCacheTypes.FLOAT.name())) {
				return input.readFloat();
			} else if (type.equalsIgnoreCase(DIHCacheTypes.DOUBLE.name())) {
				return input.readDouble();
			} else if (type.equalsIgnoreCase(DIHCacheTypes.BOOLEAN.name())) {
				return input.readBoolean();
			} else if (type.equalsIgnoreCase(DIHCacheTypes.CHARACTER.name())) {
				return input.readChar();
			} else if (type.equalsIgnoreCase(DIHCacheTypes.LONG.name())) {
				return input.readLong();
			} else {
				return input.readUTF();
			}
		} catch (IOException e) {
			LOG.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}



}
