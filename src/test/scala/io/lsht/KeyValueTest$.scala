package io.lsht

import weaver._
import cats.syntax.all._

object KeyValueTest$ extends SimpleIOSuite {

  pureTest("key equality") {
    val bytes = "hi".getBytes
    expect(Key(bytes) == Key(bytes)) and
      expect(Key(bytes) == Key("hi".getBytes)) and
      expect(Key(bytes) != Key("he".getBytes))
  }

  pureTest("key hashcode") {
    val bytes = "hi".getBytes
    expect(Key(bytes).hashCode() == Key(bytes).hashCode()) and
      expect(Key(bytes).hashCode() == Key("hi".getBytes).hashCode()) and
      expect(Key(bytes).hashCode() != Key("he".getBytes).hashCode())
  }

  pureTest("key can be used in a Set") {
    val set0 = Set.empty[Key]
    val k1 = Key("k1".getBytes)
    val set1 = set0 + k1
    val set = (2 to 6).foldLeft(set1) { case (s, i) => s + Key(s"k$i".getBytes) }
    expect(!set0.contains(k1)) and
      expect(set1.contains(k1)) and
      forEach((1 to 6).toList) { i => expect(set.contains(Key(s"k$i".getBytes))) }
  }
}
