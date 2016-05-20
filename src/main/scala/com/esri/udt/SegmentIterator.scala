package com.esri.udt

/**
  */
case class SegmentIterator(poly: PolyType) extends Iterator[Segment] {

  private var i: Int = 0
  private val m = poly.xyNum.length

  private var j: Int = 1
  private var n: Int = if (poly.xyNum.isEmpty) 0 else poly.xyNum.head

  private var a: Int = 0
  private var b: Int = 3

  private var hasMore = i < m && j < n

  override def hasNext() = {
    hasMore
  }

  override def next() = {
    val ax = poly.xyArr(a)
    val ay = poly.xyArr(a + 1)
    val am = poly.xyArr(a + 2)

    val bx = poly.xyArr(b)
    val by = poly.xyArr(b + 1)
    val bm = poly.xyArr(b + 2)

    a += 3
    b += 3
    j += 1

    // End of part ?
    if (j == n) {
      j = 1
      i += 1
      // End of all parts ?
      if (i < m) {
        n = poly.xyNum(i)
        a += 3
        b += 3
      }
      else {
        hasMore = false
      }
    }

    Segment(ax, ay, am, bx, by, bm)
  }
}
