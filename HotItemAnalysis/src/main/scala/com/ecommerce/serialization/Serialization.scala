package com.ecommerce.serialization

class Serialization {
  protected def serialization(line: String): Array[Byte] = {
    line.getBytes()
  }
}
