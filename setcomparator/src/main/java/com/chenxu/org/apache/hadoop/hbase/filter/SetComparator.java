package com.chenxu.org.apache.hadoop.hbase.filter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

import com.chenxu.org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import com.google.protobuf.InvalidProtocolBufferException;
public class SetComparator extends ByteArrayComparable {
	static final Log LOG = LogFactory.getLog(SetComparator.class);
	String value;
	public SetComparator(String value) {
		super(Bytes.toBytes(value));
		this.value = value;
	}

	@Override
	public byte[] toByteArray() {
		 ComparatorProtos.SetComparator.Builder builder =
		          ComparatorProtos.SetComparator.newBuilder();
		      builder.setClientSet(value);
		      return builder.build().toByteArray();
	}

	@Override
	public int compareTo(byte[] value, int offset, int length) {
		int code = 0;
		ByteArrayInputStream dbbas = new ByteArrayInputStream(Arrays.copyOfRange(value, offset, offset + length));
		
		HashSet<String> dbSet = null;
		HashSet<String> clientSet = null;
		ObjectInputStream dbois = null;

		try {
			dbois = new ObjectInputStream(dbbas);
			dbSet = (HashSet<String>)dbois.readObject();
			LOG.info("dbSet:"+dbSet);
			
			String[] split = this.value.split(",");
			clientSet = new HashSet<String>();
			for(String geo : split) {
				clientSet.add(geo);
			}

			LOG.info("clientSet:"+clientSet);
			
			code = dbSet.containsAll(clientSet)==true? 1: -1;
			LOG.info("code["+code+"]");
		} catch (IOException e) {
			LOG.error("dbSet:"+dbSet+"  "+"clientSet:"+clientSet,e);
		} catch (ClassNotFoundException e) {
			LOG.error("dbSet:"+dbSet+"  "+"clientSet:"+clientSet,e);
		} finally {
			if(null!=dbois) {
				try {
					dbois.close();
				} catch (IOException e) {
				}
			}
			
		}
		
		return code;
	}
	
	public static ByteArrayComparable parseFrom(final byte [] pbBytes) throws DeserializationException {

		ComparatorProtos.SetComparator proto = null;
		try {
			proto = ComparatorProtos.SetComparator.parseFrom(pbBytes);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new SetComparator(proto.getClientSet());
		
	}

}
