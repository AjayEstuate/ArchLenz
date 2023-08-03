package com.archlens.security;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class ArchLensSecurity {

	private static final String AES_ALGORITHM = "AES";
	private static final String AES_TRANSFORMATION = "AES/CBC/PKCS5Padding";
	private static final int AES_KEY_SIZE = 256;

	// Replace this key with your secure key in a real application
	private static final String SECRET_KEY = "0123456789abcdef0123456789abcdef";

	public static String encrypt(String plaintext)  {
		byte[] combined = null;
		try { 
			byte[] keyBytes = hexStringToByteArray(SECRET_KEY);
			SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, AES_ALGORITHM);

			// Generate a random initialization vector (IV)
			byte[] ivBytes = new byte[16]; // 16 bytes for AES
			new SecureRandom().nextBytes(ivBytes);
			IvParameterSpec iv = new IvParameterSpec(ivBytes);

			Cipher cipher = Cipher.getInstance(AES_TRANSFORMATION);
			cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, iv);
			byte[] encryptedBytes = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

			// Combine IV and encrypted data for storage or transmission
			combined = new byte[ivBytes.length + encryptedBytes.length];
			System.arraycopy(ivBytes, 0, combined, 0, ivBytes.length);
			System.arraycopy(encryptedBytes, 0, combined, ivBytes.length, encryptedBytes.length);
		}catch (Exception e) {
			e.printStackTrace();

		}

		// Encode the combined byte array as a Base64 string for easy storage or transmission
		return Base64.getEncoder().encodeToString(combined);
	}


	public static String decrypt(String ciphertext)  { 
		byte[] decryptedBytes = null;
		try {

			byte[] keyBytes = hexStringToByteArray(SECRET_KEY);
			SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, AES_ALGORITHM);

			// Decode the Base64 string to retrieve the combined IV and encrypted data
			byte[] combined = Base64.getDecoder().decode(ciphertext);

			// Extract IV and encrypted data from the combined byte array
			byte[] ivBytes = new byte[16];
			byte[] encryptedBytes = new byte[combined.length - 16];
			System.arraycopy(combined, 0, ivBytes, 0, ivBytes.length);
			System.arraycopy(combined, ivBytes.length, encryptedBytes, 0, encryptedBytes.length);
			IvParameterSpec iv = new IvParameterSpec(ivBytes);

			Cipher cipher = Cipher.getInstance(AES_TRANSFORMATION);
			cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, iv);
			decryptedBytes = cipher.doFinal(encryptedBytes);
		} catch (Exception e) {
			// TODO: handle exception
		}
		return new String(decryptedBytes, StandardCharsets.UTF_8);
	}

	private static byte[] hexStringToByteArray(String hexString) {
		int len = hexString.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
					+ Character.digit(hexString.charAt(i + 1), 16));
		}
		return data;
	}



}
