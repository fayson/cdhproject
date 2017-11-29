package com.cloudera.utils;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;

public class BeanAndFile {
	public static void bean2File(Object object, String file) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			byte[] byteArray = baos.toByteArray();
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(file), false));
			for (int i = 0; i < byteArray.length; i++) {
				if (i != byteArray.length - 1) {
					bw.write(byteArray[i] + "&&");
				} else {
					bw.write(byteArray[i] + "");
				}
				
			}
			bw.close();
			oos.close();
			baos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static String bean2Str(Object object) {
		StringBuffer sb = new StringBuffer();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			byte[] byteArray = baos.toByteArray();
			for (int i = 0; i < byteArray.length; i++) {
				if (i != byteArray.length - 1) {
					sb.append(byteArray[i] + "&&");
				} else {
					sb.append(byteArray[i] + "");
				}
				
			}
			oos.close();
//			sb.append(Base64.encodeBase64String(baos.toByteArray()));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("#####BeanAndFile###bean2Str:"+baos.toByteArray().length);
		}
		
		return sb.toString();
	}
}
