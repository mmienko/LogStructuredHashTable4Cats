package io.lsht

import java.nio.charset.StandardCharsets

object ByteUtils {

  private val HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII)

  /**
    * Mainly for debugging source and test
    * @param bytes
    * @return
    */
  def bytesToHex(bytes: Array[Byte]): String = {
    val hexChars = new Array[Byte](bytes.length * 2)
    for (j <- 0 until bytes.length) {
      val v = bytes(j) & 0xff
      hexChars(j * 2) = HEX_ARRAY(v >>> 4)
      hexChars(j * 2 + 1) = HEX_ARRAY(v & 0x0f)
    }
    new String(hexChars, StandardCharsets.UTF_8)
  }
}
