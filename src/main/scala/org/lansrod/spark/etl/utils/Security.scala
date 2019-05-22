package org.lansrod.spark.etl.utils

import com.google.common.base.Charsets
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64

object Security {
  def uncrypt(password: String, salt: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(salt.getBytes(Charsets.UTF_8), "AES"))
    new String(cipher.doFinal(Base64.decodeBase64(password)), Charsets.UTF_8)
  }

  def encrypt(password: String, salt: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(salt.getBytes(Charsets.UTF_8), "AES"))
    new String(Base64.encodeBase64URLSafeString(cipher.doFinal(password.getBytes(Charsets.UTF_8))))
  }

  def uncryptWithPadding(password: String, salt: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(salt.getBytes(Charsets.UTF_8), "AES"))
    new String(cipher.doFinal(Base64.decodeBase64(password)), Charsets.UTF_8)
  }

  def encryptWithPadding(password: String, salt: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(salt.getBytes(Charsets.UTF_8), "AES"))
    new String(Base64.encodeBase64URLSafeString(cipher.doFinal(password.getBytes(Charsets.UTF_8))))
  }
}
