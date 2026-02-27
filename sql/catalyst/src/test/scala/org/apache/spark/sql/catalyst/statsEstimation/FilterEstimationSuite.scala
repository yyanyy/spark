/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.statsEstimation

import java.sql.Date
import java.util
import java.util.OptionalLong

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ColumnStatsMap
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.{Scan, Statistics => V2Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.colstats.{ColumnStatistics,
  Histogram => V2Histogram, HistogramBin => V2HistogramBin}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * In this test suite, we test predicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */
class FilterEstimationSuite extends StatsEstimationTestBase {

  // Suppose our test table has 10 rows and 10 columns.
  // column cint has values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val attrInt = AttributeReference("cint", IntegerType)()
  val colStatInt = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cbool has only 2 distinct values
  val attrBool = AttributeReference("cbool", BooleanType)()
  val colStatBool = ColumnStat(distinctCount = Some(2), min = Some(false), max = Some(true),
    nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))

  // column cdate has 10 values from 2017-01-01 through 2017-01-10.
  val dMin = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-01"))
  val dMax = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-10"))
  val attrDate = AttributeReference("cdate", DateType)()
  val colStatDate = ColumnStat(distinctCount = Some(10),
    min = Some(dMin), max = Some(dMax),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cdecimal has 4 values from 0.20 through 0.80 at increment of 0.20.
  val decMin = Decimal("0.200000000000000000")
  val decMax = Decimal("0.800000000000000000")
  val attrDecimal = AttributeReference("cdecimal", DecimalType(18, 18))()
  val colStatDecimal = ColumnStat(distinctCount = Some(4),
    min = Some(decMin), max = Some(decMax),
    nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))

  // column cdouble has 10 double values: 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0
  val attrDouble = AttributeReference("cdouble", DoubleType)()
  val colStatDouble = ColumnStat(distinctCount = Some(10), min = Some(1.0), max = Some(10.0),
    nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))

  // column cstring has 10 String values:
  // "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"
  val attrString = AttributeReference("cstring", StringType)()
  val colStatString = ColumnStat(distinctCount = Some(10), min = None, max = None,
    nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))

  // column cint2 has values: 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
  // Hence, distinctCount:10, min:7, max:16, nullCount:0, avgLen:4, maxLen:4
  // This column is created to test "cint < cint2
  val attrInt2 = AttributeReference("cint2", IntegerType)()
  val colStatInt2 = ColumnStat(distinctCount = Some(10), min = Some(7), max = Some(16),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cint3 has values: 30, 31, 32, 33, 34, 35, 36, 37, 38, 39
  // Hence, distinctCount:10, min:30, max:39, nullCount:0, avgLen:4, maxLen:4
  // This column is created to test "cint = cint3 without overlap at all.
  val attrInt3 = AttributeReference("cint3", IntegerType)()
  val colStatInt3 = ColumnStat(distinctCount = Some(10), min = Some(30), max = Some(39),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cint4 has values in the range from 1 to 10
  // distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  // This column is created to test complete overlap
  val attrInt4 = AttributeReference("cint4", IntegerType)()
  val colStatInt4 = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cintHgm has values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 with histogram.
  // Note that cintHgm has an even distribution with histogram information built.
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val attrIntHgm = AttributeReference("cintHgm", IntegerType)()
  val hgmInt = Histogram(2.0, Array(HistogramBin(1.0, 2.0, 2),
    HistogramBin(2.0, 4.0, 2), HistogramBin(4.0, 6.0, 2),
    HistogramBin(6.0, 8.0, 2), HistogramBin(8.0, 10.0, 2)))
  val colStatIntHgm = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))

  // column cintSkewHgm has values: 1, 4, 4, 5, 5, 5, 5, 6, 6, 10 with histogram.
  // Note that cintSkewHgm has a skewed distribution with histogram information built.
  // distinctCount:5, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val attrIntSkewHgm = AttributeReference("cintSkewHgm", IntegerType)()
  val hgmIntSkew = Histogram(2.0, Array(HistogramBin(1.0, 4.0, 2),
    HistogramBin(4.0, 5.0, 2), HistogramBin(5.0, 5.0, 1),
    HistogramBin(5.0, 6.0, 2), HistogramBin(6.0, 10.0, 2)))
  val colStatIntSkewHgm = ColumnStat(distinctCount = Some(5), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))

  val attributeMap = AttributeMap(Seq(
    attrInt -> colStatInt,
    attrBool -> colStatBool,
    attrDate -> colStatDate,
    attrDecimal -> colStatDecimal,
    attrDouble -> colStatDouble,
    attrString -> colStatString,
    attrInt2 -> colStatInt2,
    attrInt3 -> colStatInt3,
    attrInt4 -> colStatInt4,
    attrIntHgm -> colStatIntHgm,
    attrIntSkewHgm -> colStatIntSkewHgm
  ))

  test("true") {
    validateEstimatedStats(
      Filter(TrueLiteral, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt),
      expectedRowCount = 10)
  }

  test("false") {
    validateEstimatedStats(
      Filter(FalseLiteral, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("null") {
    validateEstimatedStats(
      Filter(Literal(null, IntegerType), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("Not(null)") {
    validateEstimatedStats(
      Filter(Not(Literal(null, IntegerType)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("Not(Not(null))") {
    validateEstimatedStats(
      Filter(Not(Not(Literal(null, IntegerType))), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint < 3 AND null") {
    val condition = And(LessThan(attrInt, Literal(3)), Literal(null, IntegerType))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint < 3 OR null") {
    val condition = Or(LessThan(attrInt, Literal(3)), Literal(null, IntegerType))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(3))),
      expectedRowCount = 3)
  }

  test("Not(cint < 3 AND null)") {
    val condition = Not(And(LessThan(attrInt, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(8))),
      expectedRowCount = 8)
  }

  test("Not(cint < 3 OR null)") {
    val condition = Not(Or(LessThan(attrInt, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("Not(cint < 3 AND Not(null))") {
    val condition = Not(And(LessThan(attrInt, Literal(3)), Not(Literal(null, IntegerType))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(8))),
      expectedRowCount = 8)
  }

  test("cint = 2") {
    validateEstimatedStats(
      Filter(EqualTo(attrInt, Literal(2)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(1), min = Some(2), max = Some(2),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 1)
  }

  test("cint <=> 2") {
    validateEstimatedStats(
      Filter(EqualNullSafe(attrInt, Literal(2)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(1), min = Some(2), max = Some(2),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 1)
  }

  test("cint = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      Filter(EqualTo(attrInt, Literal(0)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint < 3") {
    validateEstimatedStats(
      Filter(LessThan(attrInt, Literal(3)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("cint < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      Filter(LessThan(attrInt, Literal(0)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint <= 3") {
    validateEstimatedStats(
      Filter(LessThanOrEqual(attrInt, Literal(3)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("cint > 6") {
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, Literal(6)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(5), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 5)
  }

  test("cint > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, Literal(10)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint >= 6") {
    validateEstimatedStats(
      Filter(GreaterThanOrEqual(attrInt, Literal(6)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(5), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 5)
  }

  test("cint IS NULL") {
    validateEstimatedStats(
      Filter(IsNull(attrInt), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint IS NOT NULL") {
    validateEstimatedStats(
      Filter(IsNotNull(attrInt), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 10)
  }

  test("cint IS NOT NULL && null") {
    // 'cint < null' will be optimized to 'cint IS NOT NULL && null'.
    // More similar cases can be found in the Optimizer NullPropagation.
    val condition = And(IsNotNull(attrInt), Literal(null, IntegerType))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint > 3 AND cint <= 6") {
    val condition = And(GreaterThan(attrInt, Literal(3)), LessThanOrEqual(attrInt, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(3), max = Some(6),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint = 3 OR cint = 6") {
    val condition = Or(EqualTo(attrInt, Literal(3)), EqualTo(attrInt, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(2))),
      expectedRowCount = 2)
  }

  test("Not(cint > 3 AND cint <= 6)") {
    val condition = Not(And(GreaterThan(attrInt, Literal(3)), LessThanOrEqual(attrInt, Literal(6))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(6))),
      expectedRowCount = 6)
  }

  test("Not(cint <= 3 OR cint > 6)") {
    val condition = Not(Or(LessThanOrEqual(attrInt, Literal(3)), GreaterThan(attrInt, Literal(6))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(5))),
      expectedRowCount = 5)
  }

  test("Not(cint = 3 AND cstring < 'A8')") {
    val condition = Not(And(EqualTo(attrInt, Literal(3)), LessThan(attrString, Literal("A8"))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt, attrString), 10L)),
      Seq(attrInt -> colStatInt, attrString -> colStatString),
      expectedRowCount = 10)
  }

  test("Not(cint = 3 OR cstring < 'A8')") {
    val condition = Not(Or(EqualTo(attrInt, Literal(3)), LessThan(attrString, Literal("A8"))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt, attrString), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(9)),
        attrString -> colStatString.copy(distinctCount = Some(9))),
      expectedRowCount = 9)
  }

  test("cint IN (3, 4, 5)") {
    validateEstimatedStats(
      Filter(InSet(attrInt, Set(3, 4, 5)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(3), min = Some(3), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("evaluateInSet with all zeros") {
    validateEstimatedStats(
      Filter(InSet(attrString, Set(3, 4, 5)),
        StatsTestPlan(Seq(attrString), 0,
          AttributeMap(Seq(attrString ->
            ColumnStat(distinctCount = Some(0), min = None, max = None,
              nullCount = Some(0), avgLen = Some(0), maxLen = Some(0)))))),
      Seq(attrString -> ColumnStat(distinctCount = Some(0))),
      expectedRowCount = 0)
  }

  test("evaluateInSet with string") {
    validateEstimatedStats(
      Filter(InSet(attrString, Set("A0")),
        StatsTestPlan(Seq(attrString), 10,
          AttributeMap(Seq(attrString ->
            ColumnStat(distinctCount = Some(10), min = None, max = None,
              nullCount = Some(0), avgLen = Some(2), maxLen = Some(2)))))),
      Seq(attrString -> ColumnStat(distinctCount = Some(1), min = None, max = None,
        nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))),
      expectedRowCount = 1)
  }

  test("cint NOT IN (3, 4, 5)") {
    validateEstimatedStats(
      Filter(Not(InSet(attrInt, Set(3, 4, 5))), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(7))),
      expectedRowCount = 7)
  }

  test("cbool IN (true)") {
    validateEstimatedStats(
      Filter(InSet(attrBool, Set(true)), childStatsTestPlan(Seq(attrBool), 10L)),
      Seq(attrBool -> ColumnStat(distinctCount = Some(1), min = Some(true), max = Some(true),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))),
      expectedRowCount = 5)
  }

  test("cbool = true") {
    validateEstimatedStats(
      Filter(EqualTo(attrBool, Literal(true)), childStatsTestPlan(Seq(attrBool), 10L)),
      Seq(attrBool -> ColumnStat(distinctCount = Some(1), min = Some(true), max = Some(true),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))),
      expectedRowCount = 5)
  }

  test("cbool > false") {
    validateEstimatedStats(
      Filter(GreaterThan(attrBool, Literal(false)), childStatsTestPlan(Seq(attrBool), 10L)),
      Seq(attrBool -> ColumnStat(distinctCount = Some(1), min = Some(false), max = Some(true),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))),
      expectedRowCount = 5)
  }

  test("cdate = cast('2017-01-02' AS DATE)") {
    val d20170102 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-02"))
    validateEstimatedStats(
      Filter(EqualTo(attrDate, Literal(d20170102, DateType)),
        childStatsTestPlan(Seq(attrDate), 10L)),
      Seq(attrDate -> ColumnStat(distinctCount = Some(1),
        min = Some(d20170102), max = Some(d20170102),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 1)
  }

  test("cdate < cast('2017-01-03' AS DATE)") {
    val d20170101 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-01"))
    val d20170103 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-03"))
    validateEstimatedStats(
      Filter(LessThan(attrDate, Literal(d20170103, DateType)),
        childStatsTestPlan(Seq(attrDate), 10L)),
      Seq(attrDate -> ColumnStat(distinctCount = Some(3),
        min = Some(d20170101), max = Some(d20170103),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("""cdate IN ( cast('2017-01-03' AS DATE),
      cast('2017-01-04' AS DATE), cast('2017-01-05' AS DATE) )""") {
    val d20170103 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-03"))
    val d20170104 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-04"))
    val d20170105 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-05"))
    validateEstimatedStats(
      Filter(In(attrDate, Seq(Literal(d20170103, DateType), Literal(d20170104, DateType),
        Literal(d20170105, DateType))), childStatsTestPlan(Seq(attrDate), 10L)),
      Seq(attrDate -> ColumnStat(distinctCount = Some(3),
        min = Some(d20170103), max = Some(d20170105),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("cdecimal = 0.400000000000000000") {
    val dec_0_40 = Decimal("0.400000000000000000")
    validateEstimatedStats(
      Filter(EqualTo(attrDecimal, Literal(dec_0_40)),
        childStatsTestPlan(Seq(attrDecimal), 4L)),
      Seq(attrDecimal -> ColumnStat(distinctCount = Some(1),
        min = Some(dec_0_40), max = Some(dec_0_40),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))),
      expectedRowCount = 1)
  }

  test("cdecimal < 0.60 ") {
    val dec_0_20 = Decimal("0.200000000000000000")
    val dec_0_60 = Decimal("0.600000000000000000")
    validateEstimatedStats(
      Filter(LessThan(attrDecimal, Literal(dec_0_60)),
        childStatsTestPlan(Seq(attrDecimal), 4L)),
      Seq(attrDecimal -> ColumnStat(distinctCount = Some(3),
        min = Some(dec_0_20), max = Some(dec_0_60),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))),
      expectedRowCount = 3)
  }

  test("cdouble < 3.0") {
    validateEstimatedStats(
      Filter(LessThan(attrDouble, Literal(3.0)), childStatsTestPlan(Seq(attrDouble), 10L)),
      Seq(attrDouble -> ColumnStat(distinctCount = Some(3), min = Some(1.0), max = Some(3.0),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))),
      expectedRowCount = 3)
  }

  test("cstring = 'A2'") {
    validateEstimatedStats(
      Filter(EqualTo(attrString, Literal("A2")), childStatsTestPlan(Seq(attrString), 10L)),
      Seq(attrString -> ColumnStat(distinctCount = Some(1), min = None, max = None,
        nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))),
      expectedRowCount = 1)
  }

  test("cstring < 'A2' - unsupported condition") {
    validateEstimatedStats(
      Filter(LessThan(attrString, Literal("A2")), childStatsTestPlan(Seq(attrString), 10L)),
      Seq(attrString -> ColumnStat(distinctCount = Some(10), min = None, max = None,
        nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))),
      expectedRowCount = 10)
  }

  test("cint IN (1, 2, 3, 4, 5)") {
    // This is a corner test case.  We want to test if we can handle the case when the number of
    // valid values in IN clause is greater than the number of distinct values for a given column.
    // For example, column has only 2 distinct values 1 and 6.
    // The predicate is: column IN (1, 2, 3, 4, 5).
    val cornerChildColStatInt = ColumnStat(distinctCount = Some(2),
      min = Some(1), max = Some(6),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cornerChildStatsTestplan = StatsTestPlan(
      outputList = Seq(attrInt),
      rowCount = 2L,
      attributeStats = AttributeMap(Seq(attrInt -> cornerChildColStatInt))
    )
    validateEstimatedStats(
      Filter(InSet(attrInt, Set(1, 2, 3, 4, 5)), cornerChildStatsTestplan),
      Seq(attrInt -> ColumnStat(distinctCount = Some(2), min = Some(1), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 2)
  }

  // This is a limitation test. We should remove it after the limitation is removed.
  test("don't estimate IsNull or IsNotNull if the child is a non-leaf node") {
    val attrIntLargerRange = AttributeReference("c1", IntegerType)()
    val colStatIntLargerRange = ColumnStat(distinctCount = Some(20),
      min = Some(1), max = Some(20),
      nullCount = Some(10), avgLen = Some(4), maxLen = Some(4))
    val smallerTable = childStatsTestPlan(Seq(attrInt), 10L)
    val largerTable = StatsTestPlan(
      outputList = Seq(attrIntLargerRange),
      rowCount = 30,
      attributeStats = AttributeMap(Seq(attrIntLargerRange -> colStatIntLargerRange)))
    val nonLeafChild = Join(largerTable, smallerTable, LeftOuter,
      Some(EqualTo(attrIntLargerRange, attrInt)), JoinHint.NONE)

    Seq(IsNull(attrIntLargerRange), IsNotNull(attrIntLargerRange)).foreach { predicate =>
      validateEstimatedStats(
        Filter(predicate, nonLeafChild),
        // column stats don't change
        Seq(attrInt -> colStatInt, attrIntLargerRange -> colStatIntLargerRange),
        expectedRowCount = 30)
    }
  }

  test("cint = cint2") {
    // partial overlap case
    validateEstimatedStats(
      Filter(EqualTo(attrInt, attrInt2), childStatsTestPlan(Seq(attrInt, attrInt2), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt2 -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint > cint2") {
    // partial overlap case
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, attrInt2), childStatsTestPlan(Seq(attrInt, attrInt2), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt2 -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint < cint2") {
    // partial overlap case
    validateEstimatedStats(
      Filter(LessThan(attrInt, attrInt2), childStatsTestPlan(Seq(attrInt, attrInt2), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt2 -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(16),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint = cint4") {
    // complete overlap case
    validateEstimatedStats(
      Filter(EqualTo(attrInt, attrInt4), childStatsTestPlan(Seq(attrInt, attrInt4), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt4 -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 10)
  }

  test("cint < cint4") {
    // partial overlap case
    validateEstimatedStats(
      Filter(LessThan(attrInt, attrInt4), childStatsTestPlan(Seq(attrInt, attrInt4), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt4 -> ColumnStat(distinctCount = Some(4), min = Some(1), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint = cint3") {
    // no records qualify due to no overlap
    validateEstimatedStats(
      Filter(EqualTo(attrInt, attrInt3), childStatsTestPlan(Seq(attrInt, attrInt3), 10L)),
      Nil, // set to empty
      expectedRowCount = 0)
  }

  test("cint < cint3") {
    // all table records qualify.
    validateEstimatedStats(
      Filter(LessThan(attrInt, attrInt3), childStatsTestPlan(Seq(attrInt, attrInt3), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt3 -> ColumnStat(distinctCount = Some(10), min = Some(30), max = Some(39),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 10)
  }

  test("cint > cint3") {
    // no records qualify due to no overlap
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, attrInt3), childStatsTestPlan(Seq(attrInt, attrInt3), 10L)),
      Nil, // set to empty
      expectedRowCount = 0)
  }

  test("update ndv for columns based on overall selectivity") {
    // filter condition: cint > 3 AND cint4 <= 6
    val condition = And(GreaterThan(attrInt, Literal(3)), LessThanOrEqual(attrInt4, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt, attrInt4, attrString), 10L)),
      Seq(
        attrInt -> ColumnStat(distinctCount = Some(5), min = Some(3), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt4 -> ColumnStat(distinctCount = Some(5), min = Some(1), max = Some(6),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrString -> colStatString.copy(distinctCount = Some(5))),
      expectedRowCount = 5)
  }

  // The following test cases have histogram information collected for the test column with
  // an even distribution
  test("Not(cintHgm < 3 AND null)") {
    val condition = Not(And(LessThan(attrIntHgm, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> colStatIntHgm.copy(distinctCount = Some(7))),
      expectedRowCount = 7)
  }

  test("cintHgm = 5") {
    validateEstimatedStats(
      Filter(EqualTo(attrIntHgm, Literal(5)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(1), min = Some(5), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 1)
  }

  test("cintHgm = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      Filter(EqualTo(attrIntHgm, Literal(0)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintHgm < 3") {
    validateEstimatedStats(
      Filter(LessThan(attrIntHgm, Literal(3)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 3)
  }

  test("cintHgm < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      Filter(LessThan(attrIntHgm, Literal(0)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintHgm <= 3") {
    validateEstimatedStats(
      Filter(LessThanOrEqual(attrIntHgm, Literal(3)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 3)
  }

  test("cintHgm > 6") {
    validateEstimatedStats(
      Filter(GreaterThan(attrIntHgm, Literal(6)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(4), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 4)
  }

  test("cintHgm > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      Filter(GreaterThan(attrIntHgm, Literal(10)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintHgm >= 6") {
    validateEstimatedStats(
      Filter(GreaterThanOrEqual(attrIntHgm, Literal(6)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(5), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 5)
  }

  test("cintHgm > 3 AND cintHgm <= 6") {
    val condition = And(GreaterThan(attrIntHgm,
      Literal(3)), LessThanOrEqual(attrIntHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(4), min = Some(3), max = Some(6),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 4)
  }

  test("cintHgm = 3 OR cintHgm = 6") {
    val condition = Or(EqualTo(attrIntHgm, Literal(3)), EqualTo(attrIntHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> colStatIntHgm.copy(distinctCount = Some(3))),
      expectedRowCount = 3)
  }

  // The following test cases have histogram information collected for the test column with
  // a skewed distribution.
  test("Not(cintSkewHgm < 3 AND null)") {
    val condition = Not(And(LessThan(attrIntSkewHgm, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> colStatIntSkewHgm.copy(distinctCount = Some(5))),
      expectedRowCount = 9)
  }

  test("cintSkewHgm = 5") {
    validateEstimatedStats(
      Filter(EqualTo(attrIntSkewHgm, Literal(5)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(5), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 4)
  }

  test("cintSkewHgm = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      Filter(EqualTo(attrIntSkewHgm, Literal(0)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintSkewHgm < 3") {
    validateEstimatedStats(
      Filter(LessThan(attrIntSkewHgm, Literal(3)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 2)
  }

  test("cintSkewHgm < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      Filter(LessThan(attrIntSkewHgm, Literal(0)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintSkewHgm <= 3") {
    validateEstimatedStats(
      Filter(LessThanOrEqual(attrIntSkewHgm, Literal(3)),
        childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 2)
  }

  test("cintSkewHgm > 6") {
    validateEstimatedStats(
      Filter(GreaterThan(attrIntSkewHgm, Literal(6)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 2)
  }

  test("cintSkewHgm > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      Filter(GreaterThan(attrIntSkewHgm, Literal(10)),
        childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintSkewHgm >= 6") {
    validateEstimatedStats(
      Filter(GreaterThanOrEqual(attrIntSkewHgm, Literal(6)),
        childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(2), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 3)
  }

  test("cintSkewHgm > 3 AND cintSkewHgm <= 6") {
    val condition = And(GreaterThan(attrIntSkewHgm,
      Literal(3)), LessThanOrEqual(attrIntSkewHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(4), min = Some(3), max = Some(6),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 8)
  }

  test("cintSkewHgm = 3 OR cintSkewHgm = 6") {
    val condition = Or(EqualTo(attrIntSkewHgm, Literal(3)), EqualTo(attrIntSkewHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> colStatIntSkewHgm.copy(distinctCount = Some(2))),
      expectedRowCount = 3)
  }

  test("SPARK-36079: Null count should be no higher than row count after filter") {
    val colStatNullableString = colStatString.copy(nullCount = Some(10))
    val condition = Filter(EqualTo(attrBool, Literal(true)),
      childStatsTestPlan(Seq(attrBool, attrString), tableRowCount = 10L,
        attributeMap = AttributeMap(Seq(
          attrBool -> colStatBool, attrString -> colStatNullableString))))
    validateEstimatedStats(
      condition,
      Seq(attrBool -> colStatBool.copy(distinctCount = Some(1), min = Some(true)),
        attrString -> colStatNullableString.copy(distinctCount = Some(5), nullCount = Some(5))),
      expectedRowCount = 5)
  }

  test("SPARK-36079: Null count higher than row count") {
    val colStatNullableString = colStatString.copy(nullCount = Some(15))
    val condition = Filter(IsNotNull(attrString),
      childStatsTestPlan(Seq(attrString), tableRowCount = 10L,
        attributeMap = AttributeMap(Seq(attrString -> colStatNullableString))))
    validateEstimatedStats(
      condition,
      Seq(attrString -> colStatNullableString),
      expectedRowCount = 0)
  }

  test("SPARK-36079: Bound selectivity >= 0") {
    val colStatNullableString = colStatString.copy(nullCount = Some(-1))
    val condition = Filter(IsNotNull(attrString),
      childStatsTestPlan(Seq(attrString), tableRowCount = 10L,
        attributeMap = AttributeMap(Seq(attrString -> colStatNullableString))))
    validateEstimatedStats(
      condition,
      Seq(attrString -> colStatString),
      expectedRowCount = 10)
  }

  test("ColumnStatsMap tests") {
    val attrNoDistinct = AttributeReference("att_without_distinct", IntegerType)()
    val attrNoCount = AttributeReference("att_without_count", BooleanType)()
    val attrNoMinMax = AttributeReference("att_without_min_max", DateType)()
    val colStatNoDistinct = ColumnStat(distinctCount = None, min = Some(1), max = Some(10),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val colStatNoCount = ColumnStat(distinctCount = Some(2), min = Some(false), max = Some(true),
      nullCount = None, avgLen = Some(1), maxLen = Some(1))
    val colStatNoMinMax = ColumnStat(distinctCount = Some(1), min = None, max = None,
      nullCount = Some(1), avgLen = None, maxLen = None)
    val columnStatsMap = ColumnStatsMap(AttributeMap(Seq(
      attrNoDistinct -> colStatNoDistinct,
      attrNoCount -> colStatNoCount,
      attrNoMinMax -> colStatNoMinMax
    )))
    assert(!columnStatsMap.hasDistinctCount(attrNoDistinct))
    assert(columnStatsMap.hasDistinctCount(attrNoCount))
    assert(columnStatsMap.hasDistinctCount(attrNoMinMax))
    assert(!columnStatsMap.hasCountStats(attrNoDistinct))
    assert(!columnStatsMap.hasCountStats(attrNoCount))
    assert(columnStatsMap.hasCountStats(attrNoMinMax))
    assert(columnStatsMap.hasMinMaxStats(attrNoDistinct))
    assert(columnStatsMap.hasMinMaxStats(attrNoCount))
    assert(!columnStatsMap.hasMinMaxStats(attrNoMinMax))
  }

  private def childStatsTestPlan(
      outList: Seq[Attribute],
      tableRowCount: BigInt,
      attributeMap: AttributeMap[ColumnStat] = attributeMap): StatsTestPlan = {
    StatsTestPlan(
      outputList = outList,
      rowCount = tableRowCount,
      attributeStats = AttributeMap(outList.map(a => a -> attributeMap(a))))
  }

  // ==================== DSv2 pre-filter stats tests ====================

  /**
   * Creates a DataSourceV2ScanRelation child plan that simulates a DSv2 scan with
   * filter pushdown, reporting both pruned stats and pre-filter stats.
   * The pre-filter stats (totalRowCount + column stats) are used by FilterEstimation
   * to compute selectivity against full-table values, identical to DSv1 behavior.
   */
  private def dsv2ChildPlan(
      outList: Seq[Attribute],
      prunedRowCount: Long,
      totalRowCount: Long,
      colStats: AttributeMap[ColumnStat]): DataSourceV2ScanRelation = {
    val attrs = outList.map(_.asInstanceOf[AttributeReference])
    val testTable = new Table {
      override def name(): String = "test_dsv2_table"
      override def schema(): StructType = StructType(
        attrs.map(a => StructField(a.name, a.dataType)))
      override def capabilities(): java.util.Set[TableCapability] =
        java.util.Set.of[TableCapability]()
    }
    val v2Relation = DataSourceV2Relation(
      testTable, attrs, catalog = None, identifier = None,
      options = CaseInsensitiveStringMap.empty())

    // Build V2 column stats (used for both columnStats and columnStatsBeforeFilters)
    val v2ColStats = new util.HashMap[NamedReference, ColumnStatistics]()
    colStats.foreach { case (attr, cs) =>
      val ref = new NamedReference {
        override def fieldNames(): Array[String] = Array(attr.name)
        override def describe(): String = attr.name
      }
      val v2cs = new ColumnStatistics {
        override def distinctCount(): OptionalLong =
          cs.distinctCount.map(v => OptionalLong.of(v.toLong)).getOrElse(OptionalLong.empty())
        override def min(): java.util.Optional[Object] =
          cs.min.map(v => java.util.Optional.of(v.asInstanceOf[Object]))
            .getOrElse(java.util.Optional.empty())
        override def max(): java.util.Optional[Object] =
          cs.max.map(v => java.util.Optional.of(v.asInstanceOf[Object]))
            .getOrElse(java.util.Optional.empty())
        override def nullCount(): OptionalLong =
          cs.nullCount.map(v => OptionalLong.of(v.toLong)).getOrElse(OptionalLong.empty())
        override def avgLen(): OptionalLong =
          cs.avgLen.map(v => OptionalLong.of(v)).getOrElse(OptionalLong.empty())
        override def maxLen(): OptionalLong =
          cs.maxLen.map(v => OptionalLong.of(v)).getOrElse(OptionalLong.empty())
        override def histogram(): java.util.Optional[V2Histogram] =
          cs.histogram.map { h =>
            val v2Bins = h.bins.map { b =>
              new V2HistogramBin { def lo(): Double = b.lo; def hi(): Double = b.hi
                def ndv(): Long = b.ndv }
            }
            val hHeight = h.height
            java.util.Optional.of[V2Histogram](new V2Histogram {
              def height(): Double = hHeight; def bins(): Array[V2HistogramBin] = v2Bins })
          }.getOrElse(java.util.Optional.empty())
      }
      v2ColStats.put(ref, v2cs)
    }

    val testScan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(
          attrs.map(a => StructField(a.name, a.dataType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.of(prunedRowCount * 8)
        override def numRows(): OptionalLong = OptionalLong.of(prunedRowCount)
        override def numRowsBeforeFilters(): OptionalLong = OptionalLong.of(totalRowCount)
        override def columnStats(): util.Map[NamedReference, ColumnStatistics] = v2ColStats
        override def columnStatsBeforeFilters(): util.Map[NamedReference, ColumnStatistics] =
          v2ColStats
      }
    }

    DataSourceV2ScanRelation(v2Relation, testScan, attrs)
  }

  test("DSv2: equality selectivity with pre-filter stats") {
    // 1M total rows, 100K pruned. With pre-filter stats, estimate against full table:
    // 1M * (1/100) = 10K
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(EqualTo(dsv2Attr, Literal(50)), child)
    assert(filter.stats.rowCount.get == 10000)
  }

  test("DSv2: equality - no cap applied (estimate can exceed prunedRowCount)") {
    // 1M total rows, 5K pruned, NDV=50. Full-table estimate: 1M/50 = 20K > 5K pruned.
    // No cap for now -- we use full-table estimate directly.
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 5000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(EqualTo(dsv2Attr, Literal(50)), child)
    // 1M * (1/50) = 20K -- exceeds prunedRowCount, no cap
    assert(filter.stats.rowCount.get == 20000)
  }

  test("DSv2: InSet selectivity with pre-filter stats") {
    // 1M total, 100K pruned, NDV=100, 5 values in set.
    // 1M * (5/100) = 50K
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(InSet(dsv2Attr, Set(10, 20, 30, 40, 50)), child)
    assert(filter.stats.rowCount.get == 50000)
  }

  test("DSv2: range selectivity computed against full-table stats") {
    // With pre-filter stats, range estimates also use totalRowCount and full-table min/max.
    // 1M total, col in [1,100], filter: col < 50
    // selectivity = (50-1)/(100-1) = 49/99
    // result = ceil(1M * 49/99) = 494950
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(LessThan(dsv2Attr, Literal(50)), child)
    val expectedRowCount = math.ceil(1000000.0 * 49.0 / 99.0).toLong
    assert(filter.stats.rowCount.get == expectedRowCount)
  }

  test("DSv2: no adjustment when pre-filter stats not reported") {
    // When the scan doesn't report numRowsBeforeFilters or columnStatsBeforeFilters,
    // behavior should be identical to standard (pruned) stats.
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val attrs = Seq(dsv2Attr.asInstanceOf[AttributeReference])
    val testTable = new Table {
      override def name(): String = "test_table_no_prefilter"
      override def schema(): StructType = StructType(
        attrs.map(a => StructField(a.name, a.dataType)))
      override def capabilities(): java.util.Set[TableCapability] =
        java.util.Set.of[TableCapability]()
    }
    val v2Relation = DataSourceV2Relation(
      testTable, attrs, catalog = None, identifier = None,
      options = CaseInsensitiveStringMap.empty())

    val v2ColStats = new util.HashMap[NamedReference, ColumnStatistics]()
    val ref = new NamedReference {
      override def fieldNames(): Array[String] = Array("cint")
      override def describe(): String = "cint"
    }
    v2ColStats.put(ref, new ColumnStatistics {
      override def distinctCount(): OptionalLong = OptionalLong.of(100)
      override def min(): java.util.Optional[Object] =
        java.util.Optional.of(1.asInstanceOf[Object])
      override def max(): java.util.Optional[Object] =
        java.util.Optional.of(100.asInstanceOf[Object])
      override def nullCount(): OptionalLong = OptionalLong.of(0)
      override def avgLen(): OptionalLong = OptionalLong.of(4)
      override def maxLen(): OptionalLong = OptionalLong.of(4)
    })

    val testScan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(
          attrs.map(a => StructField(a.name, a.dataType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.of(100000L * 8)
        override def numRows(): OptionalLong = OptionalLong.of(100000L)
        // numRowsBeforeFilters and columnStatsBeforeFilters NOT overridden -- defaults to empty
        override def columnStats(): util.Map[NamedReference, ColumnStatistics] = v2ColStats
      }
    }
    val child = DataSourceV2ScanRelation(v2Relation, testScan, attrs)
    val filter = Filter(EqualTo(dsv2Attr, Literal(50)), child)

    // Without pre-filter stats: 100K * (1/100) = 1K (no adjustment)
    assert(filter.stats.rowCount.get == 1000)
  }

  test("DSv2: equality with same totalRows and prunedRows (no pruning happened)") {
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 100000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(EqualTo(dsv2Attr, Literal(50)), child)
    // totalRows == prunedRows: 100K * (1/100) = 1K
    assert(filter.stats.rowCount.get == 1000)
  }

  test("DSv2: LessThanOrEqual boundary at min with pre-filter stats") {
    // col <= 1 (literal == min): boundary case uses 1/NDV
    // 1M * (1/100) = 10K
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(LessThanOrEqual(dsv2Attr, Literal(1)), child)
    assert(filter.stats.rowCount.get == 10000)
  }

  test("DSv2: GreaterThanOrEqual boundary at max with pre-filter stats") {
    // col >= 100 (literal == max): boundary case uses 1/NDV
    // 1M * (1/100) = 10K
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2ColStat = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(GreaterThanOrEqual(dsv2Attr, Literal(100)), child)
    assert(filter.stats.rowCount.get == 10000)
  }

  test("DSv2: histogram equality with pre-filter stats") {
    // 5 bins [1,20],[20,40],[40,60],[60,80],[80,100], each ndv=10.
    // Equality col = 50: numBinsHoldingDatum = 1/10 = 0.1, numBinsHoldingEntireRange = 5.0
    // sel = 0.1 / 5.0 = 0.02, result = 1M * 0.02 = 20K
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2Histogram = Histogram(200000.0, Array(
      HistogramBin(1.0, 20.0, 10), HistogramBin(20.0, 40.0, 10),
      HistogramBin(40.0, 60.0, 10), HistogramBin(60.0, 80.0, 10),
      HistogramBin(80.0, 100.0, 10)))
    val dsv2ColStat = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(dsv2Histogram))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(EqualTo(dsv2Attr, Literal(50)), child)
    assert(filter.stats.rowCount.get == 20000)
  }

  test("DSv2: histogram range with pre-filter stats") {
    // Same histogram. Range col < 50:
    // numBinsHoldingRange = [1,20] + [20,40] + partial [40,60) = 1.0 + 1.0 + 0.5 = 2.5
    // numBinsHoldingEntireRange = 5.0, sel = 0.5
    // result = 1M * 0.5 = 500K
    val dsv2Attr = AttributeReference("cint", IntegerType)()
    val dsv2Histogram = Histogram(200000.0, Array(
      HistogramBin(1.0, 20.0, 10), HistogramBin(20.0, 40.0, 10),
      HistogramBin(40.0, 60.0, 10), HistogramBin(60.0, 80.0, 10),
      HistogramBin(80.0, 100.0, 10)))
    val dsv2ColStat = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(dsv2Histogram))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr -> dsv2ColStat)))

    val filter = Filter(LessThan(dsv2Attr, Literal(50)), child)
    assert(filter.stats.rowCount.get == 500000)
  }

  // ============ DSv2 compound filter tests ============
  // These tests verify that with pre-filter stats, compound conditions produce
  // DSv1-identical results. The selectivity is computed against full-table stats
  // (totalRowCount + pre-filter column stats), with no component tracking needed.
  //
  // Shared setup: totalRows=1M, prunedRows=100K
  // col1: NDV=100, [1,100]   col2: NDV=50, [1,50] (or NDV=100, [1,100] for range tests)

  test("DSv2 compound: AND(eq, eq)") {
    // sel = (1/100) * (1/50) = 1/5000 = 0.0002
    // result = 1M * 0.0002 = 200
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val dsv2Attr2 = AttributeReference("col2", IntegerType)()
    val dsv2ColStat2 = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1, dsv2Attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1, dsv2Attr2 -> dsv2ColStat2)))

    val filter = Filter(
      And(EqualTo(dsv2Attr1, Literal(50)), EqualTo(dsv2Attr2, Literal(25))), child)
    assert(filter.stats.rowCount.get == 200)
  }

  test("DSv2 compound: AND(eq, range)") {
    // sel = (1/100) * (70/99)
    // result = ceil(1M * 70 / 9900) = ceil(7070.7) = 7071
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val dsv2Attr2 = AttributeReference("col2", IntegerType)()
    val dsv2ColStat2 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1, dsv2Attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1, dsv2Attr2 -> dsv2ColStat2)))

    val filter = Filter(
      And(EqualTo(dsv2Attr1, Literal(50)), GreaterThan(dsv2Attr2, Literal(30))), child)
    val expectedRowCount = math.ceil(1000000.0 * (1.0 / 100.0) * (70.0 / 99.0)).toLong
    assert(filter.stats.rowCount.get == expectedRowCount)
  }

  test("DSv2 compound: AND(range, range)") {
    // sel = (70/99) * (50/99)
    // result = ceil(1M * 70 * 50 / (99*99))
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val dsv2Attr2 = AttributeReference("col2", IntegerType)()
    val dsv2ColStat2 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1, dsv2Attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1, dsv2Attr2 -> dsv2ColStat2)))

    val filter = Filter(
      And(GreaterThan(dsv2Attr1, Literal(30)), GreaterThan(dsv2Attr2, Literal(50))), child)
    val expectedRowCount = math.ceil(1000000.0 * (70.0 / 99.0) * (50.0 / 99.0)).toLong
    assert(filter.stats.rowCount.get == expectedRowCount)
  }

  test("DSv2 compound: OR(eq, eq)") {
    // p1 = 1/100, p2 = 1/50
    // OR = 1/100 + 1/50 - (1/100)(1/50) = 149/5000
    // result = ceil(1M * 149/5000) = 29800
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val dsv2Attr2 = AttributeReference("col2", IntegerType)()
    val dsv2ColStat2 = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1, dsv2Attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1, dsv2Attr2 -> dsv2ColStat2)))

    val filter = Filter(
      Or(EqualTo(dsv2Attr1, Literal(50)), EqualTo(dsv2Attr2, Literal(25))), child)
    val p1 = 1.0 / 100.0
    val p2 = 1.0 / 50.0
    val expectedRowCount = math.ceil(1000000.0 * (p1 + p2 - p1 * p2)).toLong
    assert(filter.stats.rowCount.get == expectedRowCount)
  }

  test("DSv2 compound: OR(eq, range)") {
    // p1 = 1/100, p2 = 70/99
    // OR = 1/100 + 70/99 - (1/100)(70/99) = 71/100
    // result = ceil(1M * 71/100) = 710000
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val dsv2Attr2 = AttributeReference("col2", IntegerType)()
    val dsv2ColStat2 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1, dsv2Attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1, dsv2Attr2 -> dsv2ColStat2)))

    val filter = Filter(
      Or(EqualTo(dsv2Attr1, Literal(50)), GreaterThan(dsv2Attr2, Literal(30))), child)
    val p1 = 1.0 / 100.0
    val p2 = 70.0 / 99.0
    val expectedRowCount = math.ceil(1000000.0 * (p1 + p2 - p1 * p2)).toLong
    assert(filter.stats.rowCount.get == expectedRowCount)
  }

  test("DSv2 compound: NOT(eq)") {
    // sel = 1 - 1/100 = 99/100
    // result = ceil(1M * 0.99) = 990000
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1)))

    val filter = Filter(Not(EqualTo(dsv2Attr1, Literal(50))), child)
    assert(filter.stats.rowCount.get == 990000)
  }

  test("DSv2 compound: AND(NOT(eq), eq) - DSv1-exact, no approximation") {
    // NOT(eq1): 1 - 1/100 = 99/100
    // eq2: 1/50
    // AND: (99/100)(1/50) = 99/5000
    // result = ceil(1M * 99/5000) = 19800
    // (The component-based approach gave 18000 with ~9% approximation)
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val dsv2Attr2 = AttributeReference("col2", IntegerType)()
    val dsv2ColStat2 = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1, dsv2Attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1, dsv2Attr2 -> dsv2ColStat2)))

    val filter = Filter(
      And(Not(EqualTo(dsv2Attr1, Literal(50))), EqualTo(dsv2Attr2, Literal(25))), child)
    val expectedRowCount = math.ceil(1000000.0 * (1.0 - 1.0 / 100.0) * (1.0 / 50.0)).toLong
    assert(filter.stats.rowCount.get == expectedRowCount)
  }

  // ============ DSv2 deeper nesting tests ============

  test("DSv2 nesting: AND(eq, OR(eq, range)) - 3-level deep") {
    // Tests 3-level nesting with resolved OR feeding into AND.
    // eq_col1: 1/100
    // OR: p1=1/50, p2=70/99 → 3529/4950
    // AND: (1/100) * (3529/4950) = 3529/495000
    // rows = ceil(1M * 3529/495000) = 7130
    val col1 = AttributeReference("col1", IntegerType)()
    val col2 = AttributeReference("col2", IntegerType)()
    val col3 = AttributeReference("col3", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs2 = ColumnStat(distinctCount = Some(50),
      min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs3 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(col1, col2, col3),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(
        Seq(col1 -> cs1, col2 -> cs2, col3 -> cs3)))
    val orCond = Or(EqualTo(col2, Literal(25)),
      GreaterThan(col3, Literal(30)))
    val filter = Filter(
      And(EqualTo(col1, Literal(50)), orCond), child)
    assert(filter.stats.rowCount.get == 7130)
  }

  test("DSv2 nesting: OR(AND(eq, range), eq)") {
    // AND inner: (1/100)*(70/99) = 7/990
    // eq outer: 1/50
    // OR: 7/990 + 1/50 - 7/49500 = 1333/49500
    // rows = ceil(1M * 1333/49500) = 26930
    val col1 = AttributeReference("col1", IntegerType)()
    val col2 = AttributeReference("col2", IntegerType)()
    val col3 = AttributeReference("col3", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs2 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs3 = ColumnStat(distinctCount = Some(50),
      min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(col1, col2, col3),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(
        Seq(col1 -> cs1, col2 -> cs2, col3 -> cs3)))
    val andCond = And(EqualTo(col1, Literal(50)),
      GreaterThan(col2, Literal(30)))
    val filter = Filter(
      Or(andCond, EqualTo(col3, Literal(25))), child)
    assert(filter.stats.rowCount.get == 26930)
  }

  // ============ DSv2 same-column compound tests ============

  test("DSv2 same-column: AND(range, eq) - stat mutation") {
    // Range updates col1 stats (min->30, NDV->71), eq uses updated NDV.
    // (70/99) * (1/71) = 70/7029
    // rows = ceil(1M * 70/7029) = 9959
    val col1 = AttributeReference("col1", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(col1),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(col1 -> cs1)))
    val filter = Filter(
      And(GreaterThan(col1, Literal(30)),
        EqualTo(col1, Literal(50))), child)
    assert(filter.stats.rowCount.get == 9959)
  }

  test("DSv2 same-column: OR(eq, eq)") {
    // p1 = 1/100, p2 = 1/100
    // orSel = 1/100 + 1/100 - 1/10000 = 199/10000
    // rows = ceil(1M * 199/10000) = 19900
    val col1 = AttributeReference("col1", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(col1),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(col1 -> cs1)))
    val filter = Filter(
      Or(EqualTo(col1, Literal(10)),
        EqualTo(col1, Literal(90))), child)
    assert(filter.stats.rowCount.get == 19900)
  }

  // ============ DSv2 cap + InSet + NOT(range) tests ============

  test("DSv2 compound: AND(eq, eq) with high pruning ratio") {
    // prunedRows=50K, totalRows=1M
    // col1 NDV=10, col2 NDV=100
    // sel = (1/10)*(1/100) = 1/1000
    // rows = ceil(1M/1000) = 1000
    val col1 = AttributeReference("col1", IntegerType)()
    val col2 = AttributeReference("col2", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(10),
      min = Some(1), max = Some(10),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs2 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(col1, col2),
      prunedRowCount = 50000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(col1 -> cs1, col2 -> cs2)))
    val filter = Filter(
      And(EqualTo(col1, Literal(5)),
        EqualTo(col2, Literal(50))), child)
    assert(filter.stats.rowCount.get == 1000)
  }

  test("DSv2 compound: AND(InSet, range)") {
    // InSet 5 values on col1 NDV=100: 5/100 = 1/20
    // Range on col2 (>50): 50/99
    // AND: (1/20)*(50/99) = 5/198
    // rows = ceil(1M * 5/198) = 25253
    val col1 = AttributeReference("col1", IntegerType)()
    val col2 = AttributeReference("col2", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs2 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(col1, col2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(col1 -> cs1, col2 -> cs2)))
    val filter = Filter(
      And(InSet(col1, Set(10, 20, 30, 40, 50)),
        GreaterThan(col2, Literal(50))), child)
    val expected = math.ceil(
      1000000.0 * (5.0 / 100.0) * (50.0 / 99.0)).toLong
    assert(filter.stats.rowCount.get == expected)
  }

  test("DSv2 compound: NOT(range) - just inversion") {
    // NOT(col1 > 30): 1 - 70/99 = 29/99
    // rows = ceil(1M * 29/99) = 292930
    val col1 = AttributeReference("col1", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(col1),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(col1 -> cs1)))
    val filter = Filter(
      Not(GreaterThan(col1, Literal(30))), child)
    val expected = math.ceil(1000000.0 * 29.0 / 99.0).toLong
    assert(filter.stats.rowCount.get == expected)
  }

  // ============ Non-DSv2 backward compatibility regression tests ============

  test("Non-DSv2 compound: AND(eq, eq) - backward compat") {
    // No pre-filter stats: standard calculation against 100K rows.
    // sel = (1/100)*(1/50) = 1/5000
    // rows = ceil(100K/5000) = 20
    val col1 = AttributeReference("col1", IntegerType)()
    val col2 = AttributeReference("col2", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs2 = ColumnStat(distinctCount = Some(50),
      min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = StatsTestPlan(
      outputList = Seq(col1, col2),
      rowCount = 100000,
      attributeStats = AttributeMap(
        Seq(col1 -> cs1, col2 -> cs2)))
    val filter = Filter(
      And(EqualTo(col1, Literal(50)),
        EqualTo(col2, Literal(25))), child)
    assert(filter.stats.rowCount.get == 20)
  }

  test("Non-DSv2 compound: OR(eq, range) - backward compat") {
    // p1=1/100, p2=70/99
    // orSel = 7029/9900 = 71/100
    // rows = 100K * 71/100 = 71000
    val col1 = AttributeReference("col1", IntegerType)()
    val col2 = AttributeReference("col2", IntegerType)()
    val cs1 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cs2 = ColumnStat(distinctCount = Some(100),
      min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = StatsTestPlan(
      outputList = Seq(col1, col2),
      rowCount = 100000,
      attributeStats = AttributeMap(
        Seq(col1 -> cs1, col2 -> cs2)))
    val filter = Filter(
      Or(EqualTo(col1, Literal(50)),
        GreaterThan(col2, Literal(30))), child)
    assert(filter.stats.rowCount.get == 71000)
  }

  test("DSv2 compound: NOT(AND(eq, eq)) - De Morgan: OR(NOT(eq), NOT(eq))") {
    // NOT(eq1) = 99/100, NOT(eq2) = 49/50
    // OR = 99/100 + 49/50 - (99/100)(49/50) = 4999/5000
    // result = ceil(1M * 4999/5000) = 999800
    val dsv2Attr1 = AttributeReference("col1", IntegerType)()
    val dsv2ColStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val dsv2Attr2 = AttributeReference("col2", IntegerType)()
    val dsv2ColStat2 = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val child = dsv2ChildPlan(
      outList = Seq(dsv2Attr1, dsv2Attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = AttributeMap(Seq(dsv2Attr1 -> dsv2ColStat1, dsv2Attr2 -> dsv2ColStat2)))

    val filter = Filter(
      Not(And(EqualTo(dsv2Attr1, Literal(50)), EqualTo(dsv2Attr2, Literal(25)))), child)
    val p1 = 1.0 - 1.0 / 100.0
    val p2 = 1.0 - 1.0 / 50.0
    val expectedRowCount = math.ceil(1000000.0 * (p1 + p2 - p1 * p2)).toLong
    assert(filter.stats.rowCount.get == expectedRowCount)
  }

  // ============ DSv2 vs DSv1 equivalence test ============
  // Verify that DSv2 with pre-filter stats produces the EXACT same result as DSv1.

  test("DSv2 vs DSv1: identical results for compound filter") {
    // DSv1: StatsTestPlan with full-table stats
    val attr1 = AttributeReference("col1", IntegerType)()
    val attr2 = AttributeReference("col2", IntegerType)()
    val colStat1 = ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val colStat2 = ColumnStat(distinctCount = Some(50), min = Some(1), max = Some(50),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val colStatsMap = AttributeMap(Seq(attr1 -> colStat1, attr2 -> colStat2))

    val dsv1Child = StatsTestPlan(
      outputList = Seq(attr1, attr2),
      rowCount = 1000000L,
      attributeStats = colStatsMap)

    // DSv2: same column stats, with prunedRowCount != totalRowCount
    val dsv2Child = dsv2ChildPlan(
      outList = Seq(attr1, attr2),
      prunedRowCount = 100000L,
      totalRowCount = 1000000L,
      colStats = colStatsMap)

    // Test a complex compound filter
    val condition = And(
      Or(EqualTo(attr1, Literal(50)), GreaterThan(attr2, Literal(25))),
      Not(EqualTo(attr2, Literal(10))))

    val dsv1Filter = Filter(condition, dsv1Child)
    val dsv2Filter = Filter(condition, dsv2Child)

    assert(dsv1Filter.stats.rowCount.get == dsv2Filter.stats.rowCount.get,
      s"DSv1 (${dsv1Filter.stats.rowCount.get}) != DSv2 (${dsv2Filter.stats.rowCount.get})")
  }

  private def validateEstimatedStats(
      filterNode: Filter,
      expectedColStats: Seq[(Attribute, ColumnStat)],
      expectedRowCount: Int): Unit = {

    // If the filter has a binary operator (including those nested inside AND/OR/NOT), swap the
    // sides of the attribute and the literal, reverse the operator, and then check again.
    val swappedFilter = filterNode transformExpressionsDown {
      case EqualTo(attr: Attribute, l: Literal) =>
        EqualTo(l, attr)

      case LessThan(attr: Attribute, l: Literal) =>
        GreaterThan(l, attr)
      case LessThanOrEqual(attr: Attribute, l: Literal) =>
        GreaterThanOrEqual(l, attr)

      case GreaterThan(attr: Attribute, l: Literal) =>
        LessThan(l, attr)
      case GreaterThanOrEqual(attr: Attribute, l: Literal) =>
        LessThanOrEqual(l, attr)
    }

    val testFilters = if (swappedFilter != filterNode) {
      Seq(swappedFilter, filterNode)
    } else {
      Seq(filterNode)
    }

    testFilters.foreach { filter =>
      val expectedAttributeMap = AttributeMap(expectedColStats)
      val expectedStats = Statistics(
        sizeInBytes = getOutputSize(filter.output, expectedRowCount, expectedAttributeMap),
        rowCount = Some(expectedRowCount),
        attributeStats = expectedAttributeMap)

      val filterStats = filter.stats
      assert(filterStats.sizeInBytes == expectedStats.sizeInBytes)
      assert(filterStats.rowCount == expectedStats.rowCount)
      val rowCountValue = filterStats.rowCount.getOrElse(0)
      // check the output column stats if the row count is > 0.
      // When row count is 0, the output is set to empty.
      if (rowCountValue != 0) {
        // Need to check attributeStats one by one because we may have multiple output columns.
        // Due to update operation, the output columns may be in different order.
        assert(expectedColStats.size == filterStats.attributeStats.size)
        expectedColStats.foreach { kv =>
          val filterColumnStat = filterStats.attributeStats.get(kv._1).get
          assert(filterColumnStat == kv._2)
        }
      }
    }
  }

}
