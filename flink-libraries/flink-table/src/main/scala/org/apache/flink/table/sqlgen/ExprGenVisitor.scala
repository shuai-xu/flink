/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sqlgen

import org.apache.flink.table.api._
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.types.{AtomicType, DataTypes}

class ExprGenVisitor(tableEnv: TableEnvironment) extends LogicalExprVisitor[String]{

  val functionMap = collection.mutable.Map[String, (String, Option[String])]()

  override def visit(expression: Expression): String =
    expression.accept(this)

  override def visit(sum: Sum): String =
    s"sum(${sum.child.accept(this)})"

  override def visit(sum0: Sum0): String =
    s"sum0(${sum0.child.accept(this)})"

  override def visit(min: Min): String =
    s"min(${min.child.accept(this)})"

  override def visit(max: Max): String =
    s"max(${max.child.accept(this)})"

  override def visit(count: Count): String =
    s"count(${count.child.accept(this)})"

  override def visit(avg: Avg): String =
    s"avg(${avg.child.accept(this)})"

  override def visit(lead: Lead): String =
    if (lead.children.length == 2) {
      s"lead(${lead.children.map(_.accept(this)).mkString(", ")}, null)"
    } else {
      s"lead(${lead.children.map(_.accept(this)).mkString(", ")})"
    }

  override def visit(lag: Lag): String =
    if (lag.children.length == 2) {
      s"lag(${lag.children.map(_.accept(this)).mkString(", ")}, null)"
    } else {
      s"lag(${lag.children.map(_.accept(this)).mkString(", ")})"
    }

  override def visit(stddevPop: StddevPop): String =
    s"stddev_pop(${stddevPop.child.accept(this)})"

  override def visit(stddevSamp: StddevSamp): String =
    s"stddev_samp(${stddevSamp.child.accept(this)})"

  override def visit(stddev: Stddev): String =
    s"stddev(${stddev.child.accept(this)})"

  override def visit(varPop: VarPop): String =
    s"var_pop(${varPop.child.accept(this)})"

  override def visit(varSamp: VarSamp): String =
    s"var_samp(${varSamp.child.accept(this)})"

  override def visit(variance: Variance): String =
    s"variance(${variance.child.accept(this)})"

  override def visit(firstValue: FirstValue): String =
    s"first_value(${firstValue.child.accept(this)})"

  override def visit(lastValue: LastValue): String =
    s"first_value(${lastValue.child.accept(this)})"

  override def visit(aggFunctionCall: AggFunctionCall): String = {

    val (functionId, content, comment) = SqlGenUtil
      .getFunctionIdAndContent(aggFunctionCall.aggregateFunction)

    if (!functionMap.contains(functionId)) {
      functionMap += functionId -> (content, comment)
    }

    s"$functionId" +
      s"(${aggFunctionCall.args.map(_.accept(this)).mkString(", ")})"
  }

  override def visit(singleValue: SingleValue): String =
    s"single_value(${singleValue.child.accept(this)})"

  override def visit(plus: Plus): String =
    s"(${plus.left.accept(this)}) + (${plus.right.accept(this)})"

  override def visit(unaryMinus: UnaryMinus): String =
    s"-(${unaryMinus.child.accept(this)})"

  override def visit(minus: org.apache.flink.table.expressions.Minus): String =
    s"(${minus.left.accept(this)}) - (${minus.right.accept(this)})"

  override def visit(div: Div): String =
    s"(${div.left.accept(this)}) / (${div.right.accept(this)})"

  override def visit(mul: Mul): String =
    s"(${mul.left.accept(this)}) * (${mul.right.accept(this)})"

  override def visit(mod: Mod): String =
    s"(${mod.left.accept(this)}) % (${mod.right.accept(this)})"

  override def visit(rowConstructor: RowConstructor): String =
    s"row(${rowConstructor.elements.map(_.accept(this)).mkString(", ")})"

  override def visit(arrayConstructor: ArrayConstructor): String =
    s"array(${arrayConstructor.elements.map(_.accept(this)).mkString(", ")})"

  override def visit(mapConstructor: MapConstructor): String =
    s"map(${mapConstructor.elements.map(_.accept(this)).mkString(", ")})"

  override def visit(arrayElement: ArrayElement): String =
    s"ELEMENT(${arrayElement.array.accept(this)})"

  override def visit(cardinality: Cardinality): String =
    s"cardinality(${cardinality.container.accept(this)})"

  override def visit(itemAt: ItemAt): String =
    s"key(${itemAt.container.accept(this)}, ${itemAt.index.accept(this)})"

  override def visit(call: Call): String =
    s"\\${call.functionName}(${call.args.map(_.accept(this)).mkString(", ")})"

  override def visit(unresolvedOverCall: UnresolvedOverCall): String = ???

  override def visit(overCall: OverCall): String = {
    val precedingSql = overCall.preceding.accept(this)

    val (preCount, preType) =
      (precedingSql.split(" ")(0), precedingSql.split(" ")(1))

    val prevStr = if (precedingSql.indexOf("UNBOUNDED") >= 0) {
      precedingSql
    } else if ("ROWS" == preType) {
      s"ROWS BETWEEN $preCount "
    } else {
      s"RANGE BETWEEN $precedingSql "
    }

    val followingSql = overCall.following.accept(this)

    val (followCount, followType) =
      (followingSql.split(" ")(0), followingSql.split(" ")(1))

    val followStr = if ("ROWS" == followType) {
      followCount
    } else {
      followingSql
    }

    s"${overCall.agg.accept(this)} OVER (" +
      s"PARTITION BY (${overCall.partitionBy.map(_.accept(this)).mkString(", ")}) " +
      s"ORDER BY ${overCall.orderBy.map(_.accept(this)).mkString(", ")} " +
      s"$prevStr PRECEDING" +
      s" $followStr)"
  }

  override def visit(scalarFunctionCall: ScalarFunctionCall): String = {

    val (functionId, content, comment) =
      SqlGenUtil.getFunctionIdAndContent(scalarFunctionCall.scalarFunction)

    if (!functionMap.contains(functionId)) {
      functionMap += functionId -> (content, comment)
    }
    val expr =
      s"$functionId" +
        s"(${scalarFunctionCall.parameters.map(_.accept(this)).mkString(", ")})"

    expr
  }

  override def visit(tableFunctionCall: TableFunctionCall): String = {
    val (functionId, content, comment) =
      SqlGenUtil.getFunctionIdAndContent(tableFunctionCall.tableFunction)

    if (!functionMap.contains(functionId)) {
      functionMap += functionId -> (content, comment)
    }

    val expr = s"$functionId" +
      s"(${tableFunctionCall.parameters.map(_.accept(this)).mkString(", ")})"

    expr
  }

  override def visit(throwException: ThrowException): String =
    s"ThrowException(${throwException.msg.accept(this)})"

  override def visit(cast: Cast): String =
    s"CAST(${cast.child.accept(this)} AS " +
        s"${SqlGenUtil.getTypeName(DataTypes.internal(cast.resultType))})"

  override def visit(equalTo: EqualTo): String =
    s"${equalTo.left.accept(this)} = ${equalTo.right.accept(this)}"

  override def visit(notEqualTo: NotEqualTo): String =
    s"${notEqualTo.left.accept(this)} <> ${notEqualTo.right.accept(this)}"

  override def visit(greaterThan: GreaterThan): String =
    s"${greaterThan.left.accept(this)} > ${greaterThan.right.accept(this)}"

  override def visit(greaterThanOrEqual: GreaterThanOrEqual): String =
    s"${greaterThanOrEqual.left.accept(this)} >= ${greaterThanOrEqual.right.accept(this)}"

  override def visit(lessThan: LessThan): String =
    s"${lessThan.left.accept(this)} < ${lessThan.right.accept(this)}"

  override def visit(lessThanOrEqual: LessThanOrEqual): String =
    s"${lessThanOrEqual.left.accept(this)} <= ${lessThanOrEqual.right.accept(this)}"

  override def visit(isNull: IsNull): String =
    s"(${isNull.child.accept(this)} IS NULL)"

  override def visit(isNotNull: IsNotNull): String =
    s"(${isNotNull.child.accept(this)}) IS NOT NULL"

  override def visit(isTrue: IsTrue): String =
    s"(${isTrue.child.accept(this)}) IS TRUE"

  override def visit(isNotTrue: IsNotTrue): String =
    s"(${isNotTrue.child.accept(this)}) IS NOT TRUE"

  override def visit(isFalse: IsFalse): String =
    s"(${isFalse.child.accept(this)}) IS FALSE"

  override def visit(isNotFalse: IsNotFalse): String =
    s"(${isNotFalse.child.accept(this)}) IS NOT FALSE"


  override def visit(between: Between): String =
    s"BETWEEN (${between.lowerBound.accept(this)}, ${between.upperBound.accept(this)})"

  override def visit(notBetween: NotBetween): String =
    s"NOT BETWEEN (${notBetween.lowerBound.accept(this)}, ${notBetween.upperBound.accept(this)})"

  override def visit(flattening: Flattening): String =
    s"flatten(${flattening.child.accept(this)})"

  override def visit(getCompositeField: GetCompositeField): String =
    s"get(${getCompositeField.child.accept(this)}, ${getCompositeField.key}))"

  override def visit(unresolvedFieldReference: UnresolvedFieldReference): String =
    s"`${unresolvedFieldReference.name}`"

  override def visit(resolvedFieldReference: ResolvedFieldReference): String =
    s"`${resolvedFieldReference.name}`"

  override def visit(unresolvedAggBufferReference: UnresolvedAggBufferReference): String =
    s"`${unresolvedAggBufferReference.name}`"

  override def visit(resolvedAggInputReference: ResolvedAggInputReference): String =
    s"`${resolvedAggInputReference.name}`"

  override def visit(resolvedAggBufferReference: ResolvedAggBufferReference): String =
    s"`${resolvedAggBufferReference.name}`"

  override def visit(resolvedAggLocalReference: ResolvedAggLocalReference): String =
    s"`${resolvedAggLocalReference.name}`"

  override def visit(alias: Alias): String =
    s"${alias.child.accept(this)} AS `${alias.name}`"

  override def visit(unresolvedAlias: UnresolvedAlias): String = ???

  override def visit(windowReference: WindowReference): String =
    s"`${windowReference.name}`"

  override def visit(tableReference: TableReference): String =
    s"`${tableReference.name}`"

  override def visit(rowtimeAttribute: RowtimeAttribute): String =
    s"rowtime(${rowtimeAttribute.child.accept(this)})"

  override def visit(proctimeAttribute: ProctimeAttribute): String =
    s"proctime(${proctimeAttribute.child.accept(this)})"

  override def visit(literal: Literal): String = literal.resultType match {
    case _@DataTypes.STRING => s"'${literal.value}'"
    case _@DataTypes.DATE => literal.value.toString + ".toDate"
    case _@DataTypes.TIME => literal.value.toString + ".toTime"
    case _@DataTypes.TIMESTAMP => literal.value.toString + ".toTimestamp"
    case _@DataTypes.INTERVAL_MILLIS => s"'${literal.value.toString}'" + " MILLIS"
    case _@DataTypes.INTERVAL_MONTHS => s"'${literal.value.toString}'" + " MONTH"
    case _@DataTypes.INTERVAL_ROWS => literal.value.toString + " ROWS"
    case _@DataTypes.LONG => s"${literal.value.toString}L"
    case _: AtomicType => s"${literal.value.toString}"
    case _ => s"Literal(${literal.value}, ${literal.resultType})"
  }

  override def visit(_null: Null): String = "NULL"

  override def visit(not: Not): String =
    s"NOT (${not.child.accept(this)})"

  override def visit(and: And): String =
    s"(${and.left.accept(this)}) AND (${and.right.accept(this)})"

  override def visit(or: Or): String =
    s"(${or.left.accept(this)}) OR (${or.right.accept(this)})"

  override def visit(_if: If): String =
    s"CASE WHEN (${_if.condition.accept(this)}) THEN " +
      s"${_if.ifTrue.accept(this)} ELSE ${_if.ifFalse.accept(this)} END"

  override def visit(abs: Abs): String =
    s"abs(${abs.child.accept(this)})"

  override def visit(ceil: Ceil): String =
    s"ceil(${ceil.child.accept(this)})"

  override def visit(exp: Exp): String =
    s"exp(${exp.child.accept(this)})"

  override def visit(floor: Floor): String =
    s"floor(${floor.child.accept(this)})"

  override def visit(log10: Log10): String =
    s"log10(${log10.child.accept(this)})"

  override def visit(ln: Ln): String =
    s"ln(${ln.child.accept(this)})"


  override def visit(power: Power): String =
    s"pow(${power.left.accept(this)}, ${power.right.accept(this)}})"

  override def visit(sqrt: Sqrt): String =
    s"sqrt(${sqrt.child.accept(this)})"

  override def visit(sin: Sin): String =
    s"sin(${sin.child.accept(this)})"

  override def visit(cos: Cos): String =
    s"cos(${cos.child.accept(this)})"

  override def visit(tan: Tan): String =
    s"tan(${tan.child.accept(this)})"

  override def visit(cot: Cot): String =
    s"cot(${cot.child.accept(this)})"

  override def visit(asin: Asin): String =
    s"asin(${asin.child.accept(this)})"

  override def visit(acos: Acos): String =
    s"acos(${acos.child.accept(this)})"

  override def visit(atan: Atan): String =
    s"atan(${atan.child.accept(this)})"

  override def visit(degrees: Degrees): String =
    s"degrees(${degrees.child.accept(this)})"

  override def visit(radians: Radians): String =
    s"radians(${radians.child.accept(this)})"

  override def visit(sign: Sign): String =
    s"sign(${sign.child.accept(this)})"

  override def visit(round: Round): String =
    s"round(${round.left.accept(this)}, ${round.right.accept(this)})"

  override def visit(pi: Pi): String = "pi()"

  override def visit(e: E): String = "e()"

  override def visit(rand: Rand): String = if (rand.seed != null) {
    s"RAND(${rand.seed.accept(this)})"
  } else {
    s"RAND()"
  }

  override def visit(randInteger: RandInteger): String = if (randInteger.seed != null) {
    s"RAND_INTEGER(${randInteger.seed.accept(this)}, ${randInteger.bound.accept(this)})"
  } else {
    s"RAND_INTEGER(${randInteger.bound.accept(this)})"
  }

  override def visit(asc: Asc): String =
    s"(${asc.child.accept(this)}) ASC"

  override def visit(desc: Desc): String =
    s"(${desc.child.accept(this)}) DESC"

  override def visit(nullsFirst: NullsFirst): String =
    s"(${nullsFirst.child.accept(this)}) NULLS FIRST"

  override def visit(nullsLast: NullsLast): String =
    s"(${nullsLast.child.accept(this)}) NULLS LAST"

  override def visit(charLength: CharLength): String =
    s"CHAR_LENGTH(${charLength.child.accept(this)})"

  override def visit(initCap: InitCap): String =
    s"INITCAP(${initCap.child.accept(this)})"

  override def visit(like: Like): String =
    s"(${like.str.accept(this)}) LIKE (${like.pattern.accept(this)})"

  override def visit(lower: Lower): String =
    s"LOWER(${lower.child.accept(this)})"

  override def visit(similar: Similar): String =
    s"(${similar.str.accept(this)}) SIMILAR TO (${similar.pattern.accept(this)})"

  override def visit(substring: Substring): String =
    s"SUBSTRING(${substring.str.accept(this)} FROM ${substring.begin.accept(this)} for " +
      s"${substring.length.accept(this)})"

  override def visit(trim: Trim): String =
    s"TRIM(${trim.str.accept(this)}, ${trim.trimMode.accept(this)}, " + s"${trim.trimString
                                                                            .accept(this)})"

  override def visit(ltrim: Ltrim): String =
    s"LTRIM(${ltrim.str.accept(this)}, ${ltrim.trimString.accept(this)})"

  override def visit(rtrim: Rtrim): String =
    s"RTRIM(${rtrim.str.accept(this)}, ${rtrim.trimString.accept(this)})"

  override def visit(upper: Upper): String =
    s"UPPER(${upper.child.accept(this)})"

  override def visit(position: Position): String =
    s"POSITION(${position.needle.accept(this)} IN ${position.haystack.accept(this)})"

  override def visit(overlay: Overlay): String =
    s"OVERLAY(${overlay.str.accept(this)} PLACING ${overlay.replacement.accept(this)} FROM " +
      s"${overlay.starting.accept(this)} FOR ${overlay.position.accept(this)})"

  override def visit(concat: Concat): String =
    s"concat(${concat.strings.map(_.accept(this)).mkString(", ")})"

  override def visit(concatWs: ConcatWs): String =
    s"concat_ws(${concatWs.separator.accept(this)}, " +
      s"${concatWs.strings.map(_.accept(this)).mkString(", ")})"

  override def visit(in: In): String =
    s"${in.expression.accept(this)} IN (${in.elements.map(_.accept(this)).mkString(", ")})"

  override def visit(symbolExpression: SymbolExpression): String =
    s"${symbolExpression.symbol.symbols}.${symbolExpression.symbol.name}"

  override def visit(proc: Proctime): String = "PROCTIME()"

  override def visit(extract: Extract): String =
    s"EXTRACT(${extract.timeIntervalUnit} FROM ${extract.temporal.accept(this)})"

  override def visit(temporalFloor: TemporalFloor): String =
    s"FLOOR(${temporalFloor.temporal.accept(this)} TO ${temporalFloor.timeIntervalUnit})"

  override def visit(temporalCeil: TemporalCeil): String =
    s"CEIL(${temporalCeil.temporal.accept(this)} TO ${temporalCeil.timeIntervalUnit})"

  override def visit(currentDate: CurrentDate): String = "CURRENT_DATE"

  override def visit(currentTime: CurrentTime): String = "CURRENT_TIME"

  override def visit(currentTimestamp: CurrentTimestamp): String = "CURRENT_TIMESTAMP"

  override def visit(localTime: LocalTime): String = "LOCALTIME"

  override def visit(localTimestamp: LocalTimestamp): String = "LOCALTIMESTAMP"

  override def visit(quarter: Quarter): String =
    s"QUARTER(${quarter.child.accept(this)})"

  override def visit(temporalOverlaps: TemporalOverlaps): String =
    s"temporalOverlaps(${temporalOverlaps.children.map(_.accept(this)).mkString(", ")})"

  override def visit(dateFormat: DateFormat): String =
    s"DATE_FORMAT(${dateFormat.timestamp.accept(this)}, ${dateFormat.format.accept(this)})"

  override def visit(windowStart: WindowStart): String =
    s"start(${windowStart.child.accept(this)})"

  override def visit(windowEnd: WindowEnd): String =
    s"end(${windowEnd.child.accept(this)})"

  override def visit(logicalWindow: LogicalWindow): String =
    logicalWindow.accept(this)

  override def visit(tumblingGroupWindow: TumblingGroupWindow): String =
    s"TUMBLE(${tumblingGroupWindow.timeField.accept(this)}, " +
      s"INTERVAL ${tumblingGroupWindow.size.accept(this)})"

  override def visit(slidingGroupWindow: SlidingGroupWindow): String =
    s"HOP(${slidingGroupWindow.timeField.accept(this)}, " +
      s"INTERVAL ${slidingGroupWindow.slide.accept(this)}, " +
      s"INTERVAL ${slidingGroupWindow.size.accept(this)})"

  override def visit(sessionGroupWindow: SessionGroupWindow): String =
    s"SESSION(${sessionGroupWindow.timeField.accept(this)}, " +
      s"INTERVAL ${sessionGroupWindow.gap.accept(this)})"

  override def visit(currentRow: CurrentRow): String = "AND CURRENT ROW"

  override def visit(currentRange: CurrentRange): String = "AND CURRENT ROW"

  override def visit(unboundedRow: UnboundedRow): String = "ROWS BETWEEN UNBOUNDED"

  override def visit(unboundedRange: UnboundedRange): String = "RANGE BETWEEN UNBOUNDED"

  override def visit(left: Left): String =
    s"LEFT(${left.str.accept(this)}, ${left.length.accept(this)})"

  override def visit(right: Right): String =
    s"RIGHT(${right.str.accept(this)}, ${right.length.accept(this)})"

  override def visit(locate: Locate): String =
    s"LOCATE(${locate.needle.accept(this)}, " +
        s"${locate.haystack.accept(this)}, " +
        s"${locate.starting.accept(this)})"

  override def visit(ascii: Ascii): String =
    s"Ascii(${ascii.str.accept(this)})"

  override def visit(encode: Encode): String =
    s"ENCODE(${encode.str.accept(this)}, " +
         s"${encode.charset.accept(this)})"

  override def visit(decode: Decode): String =
    s"DECODE(${decode.binary.accept(this)}, " +
        s"${decode.charset.accept(this)})"

  override def visit(instr: Instr): String =
    s"Instr(${instr.str.accept(this)}, " +
        s"${instr.subString.accept(this)}, " +
        s"${instr.startPosition.accept(this)}, " +
        s"${instr.nthAppearance.accept(this)})"

  override def visit(hash: HashExpression): String =
    s"${hash.hashName}(${hash.children.map(_.accept(this)).mkString(", ")})"
}
