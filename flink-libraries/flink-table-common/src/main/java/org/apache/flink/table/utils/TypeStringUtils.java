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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.ArrayType;
import org.apache.flink.table.types.BooleanType;
import org.apache.flink.table.types.ByteType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DateType;
import org.apache.flink.table.types.DecimalType;
import org.apache.flink.table.types.DoubleType;
import org.apache.flink.table.types.FloatType;
import org.apache.flink.table.types.GenericType;
import org.apache.flink.table.types.IntType;
import org.apache.flink.table.types.InternalType;
import org.apache.flink.table.types.LongType;
import org.apache.flink.table.types.MapType;
import org.apache.flink.table.types.MultisetType;
import org.apache.flink.table.types.RowType;
import org.apache.flink.table.types.ShortType;
import org.apache.flink.table.types.StringType;
import org.apache.flink.table.types.TimeType;
import org.apache.flink.table.types.TimestampType;

import java.util.ArrayList;
import java.util.List;

/**
  * Utilities to convert {@link TypeInformation} into a string representation and back.
  */
@PublicEvolving
public class TypeStringUtils {

	private static final String VARCHAR = "VARCHAR";
	private static final String STRING = "STRING";
	private static final String BOOLEAN = "BOOLEAN";
	private static final String BYTE = "BYTE";
	private static final String TINYINT = "TINYINT";
	private static final String SHORT = "SHORT";
	private static final String SMALLINT = "SMALLINT";
	private static final String INT = "INT";
	private static final String LONG = "LONG";
	private static final String BIGINT = "BIGINT";
	private static final String FLOAT = "FLOAT";
	private static final String DOUBLE = "DOUBLE";
	private static final String DECIMAL = "DECIMAL";
	private static final String SQL_DATE = "SQL_DATE";
	private static final String DATE = "DATE";
	private static final String SQL_TIME = "SQL_TIME";
	private static final String TIME = "TIME";
	private static final String SQL_TIMESTAMP = "SQL_TIMESTAMP";
	private static final String TIMESTAMP = "TIMESTAMP";
	private static final String ROW = "ROW";
	private static final String ANY = "ANY";
	private static final String POJO = "POJO";
	private static final String MAP = "MAP";
	private static final String MULTISET = "MULTISET";
	private static final String PRIMITIVE_ARRAY = "PRIMITIVE_ARRAY";
	private static final String OBJECT_ARRAY = "OBJECT_ARRAY";

	public static String writeTypeInfo(TypeInformation<?> typeInfo) {
		if (typeInfo.equals(Types.STRING)) {
			return VARCHAR;
		} else if (typeInfo.equals(Types.BOOLEAN)) {
			return BOOLEAN;
		} else if (typeInfo.equals(Types.BYTE)) {
			return TINYINT;
		} else if (typeInfo.equals(Types.SHORT)) {
			return SMALLINT;
		} else if (typeInfo.equals(Types.INT)) {
			return INT;
		} else if (typeInfo.equals(Types.LONG)) {
			return BIGINT;
		} else if (typeInfo.equals(Types.FLOAT)) {
			return FLOAT;
		} else if (typeInfo.equals(Types.DOUBLE)) {
			return DOUBLE;
		} else if (typeInfo.equals(Types.BIG_DEC)) {
			return DECIMAL;
		} else if (typeInfo.equals(Types.SQL_DATE)) {
			return DATE;
		} else if (typeInfo.equals(Types.SQL_TIME)) {
			return TIME;
		} else if (typeInfo.equals(Types.SQL_TIMESTAMP)) {
			return TIMESTAMP;
		} else if (typeInfo instanceof RowTypeInfo) {
			final RowTypeInfo rt = (RowTypeInfo) typeInfo;
			final String[] fieldNames = rt.getFieldNames();
			final TypeInformation<?>[] fieldTypes = rt.getFieldTypes();

			final StringBuilder result = new StringBuilder();
			result.append(ROW);
			result.append('<');
			for (int i = 0; i < fieldNames.length; i++) {
				// escape field name if it contains delimiters
				if (containsDelimiter(fieldNames[i])) {
					result.append('`');
					result.append(fieldNames[i].replace("`", "``"));
					result.append('`');
				} else {
					result.append(fieldNames[i]);
				}
				result.append(' ');
				result.append(writeTypeInfo(fieldTypes[i]));
				if (i < fieldNames.length - 1) {
					result.append(", ");
				}
			}
			result.append('>');
			return result.toString();
		} else if (typeInfo instanceof GenericTypeInfo) {
			return ANY + '<' + typeInfo.getTypeClass().getName() + '>';
		} else if (typeInfo instanceof PojoTypeInfo) {
			// we only support very simple POJOs that only contain extracted fields
			// (not manually specified)
			TypeInformation<?> extractedPojo;
			try {
				extractedPojo = TypeExtractor.createTypeInfo(typeInfo.getTypeClass());
			} catch (InvalidTypesException e) {
				extractedPojo = null;
			}
			if (extractedPojo == null || !typeInfo.equals(extractedPojo)) {
				throw new TableException(
					"A string representation for custom POJO types is not supported yet.");
			}
			return POJO + '<' + typeInfo.getTypeClass().getName() + '>';
		} else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
			final PrimitiveArrayTypeInfo arrayTypeInfo = (PrimitiveArrayTypeInfo) typeInfo;
			return PRIMITIVE_ARRAY + '<' + writeTypeInfo(arrayTypeInfo.getComponentType()) + '>';
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			final ObjectArrayTypeInfo arrayTypeInfo = (ObjectArrayTypeInfo) typeInfo;
			return OBJECT_ARRAY + '<' + writeTypeInfo(arrayTypeInfo.getComponentInfo()) + '>';
		} else if (typeInfo instanceof MultisetTypeInfo) {
			final MultisetTypeInfo multisetTypeInfo = (MultisetTypeInfo) typeInfo;
			return MULTISET + '<' + writeTypeInfo(multisetTypeInfo.getElementTypeInfo()) + '>';
		} else if (typeInfo instanceof MapTypeInfo) {
			final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
			final String keyTypeInfo = writeTypeInfo(mapTypeInfo.getKeyTypeInfo());
			final String valueTypeInfo = writeTypeInfo(mapTypeInfo.getValueTypeInfo());
			return MAP + '<' + keyTypeInfo + ", " + valueTypeInfo + '>';
		} else {
			return ANY + '<' + typeInfo.getTypeClass().getName() + ", " +
				EncodingUtils.encodeObjectToString(typeInfo) + '>';
		}
	}

	public static String writeDataType(DataType dataType) {
		if (dataType instanceof StringType) {
			return VARCHAR;
		} else if (dataType instanceof BooleanType) {
			return BOOLEAN;
		} else if (dataType instanceof ByteType) {
			return BYTE;
		} else if (dataType instanceof ShortType) {
			return SHORT;
		} else if (dataType instanceof IntType) {
			return INT;
		} else if (dataType instanceof LongType) {
			return LONG;
		} else if (dataType instanceof FloatType) {
			return FLOAT;
		} else if (dataType instanceof DoubleType) {
			return DOUBLE;
		} else if (dataType instanceof DecimalType) {
			//TODO: support precision and scale
			return DECIMAL;
		} else if (dataType instanceof DateType) {
			return SQL_DATE;
		} else if (dataType instanceof TimeType) {
			return SQL_TIME;
		} else if (dataType instanceof TimestampType) {
			return SQL_TIMESTAMP;
		} else if (dataType instanceof RowType) {
			final RowType rt = (RowType) dataType;
			final String[] fieldNames = rt.getFieldNames();
			final DataType[] fieldTypes = rt.getFieldTypes();

			final StringBuilder result = new StringBuilder();
			result.append(ROW);
			result.append('<');
			for (int i = 0; i < fieldNames.length; i++) {
				// escape field name if it contains delimiters
				if (containsDelimiter(fieldNames[i])) {
					result.append('`');
					result.append(fieldNames[i].replace("`", "``"));
					result.append('`');
				} else {
					result.append(fieldNames[i]);
				}
				result.append(' ');
				result.append(writeDataType(fieldTypes[i]));
				if (i < fieldNames.length - 1) {
					result.append(", ");
				}
			}
			result.append('>');
			return result.toString();
		} else if (dataType instanceof GenericType) {
			String clazzName = ((GenericType) dataType).getTypeClass().getName();
			return ANY + '<' + clazzName + '>';
		} else if (dataType instanceof MultisetType) {
			final DataType elementType = ((MultisetType) dataType).getElementType();
			return MULTISET + '<' + writeDataType(elementType) + '>';
		}  else if (dataType instanceof MapType) {
			final DataType keyType = ((MapType) dataType).getKeyType();
			final DataType valueType = ((MapType) dataType).getValueType();
			return MAP + '<' + writeDataType(keyType) + ", " + writeDataType(valueType) + '>';
		} else if (dataType instanceof ArrayType) {
			final boolean isPrimitive = ((ArrayType) dataType).isPrimitive();
			final DataType elementType = ((ArrayType) dataType).getElementType();
			final String result = isPrimitive ? PRIMITIVE_ARRAY : OBJECT_ARRAY;
			return result + '<' + writeDataType(elementType) + '>';
		} else {
			return dataType.toString();
		}
	}

	public static TypeInformation<?> readTypeInfo(String typeString) {
		final List<Token> tokens = tokenize(typeString);
		final TokenConverter converter = new TokenConverter(typeString, tokens);
		return converter.convert();
	}

	public static InternalType readDataType(String typeString) {
		final List<Token> tokens = tokenize(typeString);
		final TokenConverter converter = new TokenConverter(typeString, tokens);
		return converter.convert2();
	}

	// --------------------------------------------------------------------------------------------

	private static boolean containsDelimiter(String string) {
		final char[] charArray = string.toCharArray();
		for (char c : charArray) {
			if (isDelimiter(c)) {
				return true;
			}
		}
		return false;
	}

	private static boolean isDelimiter(char character) {
		return Character.isWhitespace(character) || character == ',' ||
			character == '<' || character == '>' || character == '(' || character == ')';
	}

	private static List<Token> tokenize(String typeString) {
		final char[] chars = typeString.toCharArray();

		final List<Token> tokens = new ArrayList<>();
		for (int cursor = 0; cursor < chars.length; cursor++) {
			char curChar = chars[cursor];
			if (curChar == '<' || curChar == '(') {
				tokens.add(new Token(TokenType.BEGIN, Character.toString(curChar), cursor));
			} else if (curChar == '>' || curChar == ')') {
				tokens.add(new Token(TokenType.END, Character.toString(curChar), cursor));
			} else if (curChar == ',') {
				tokens.add(new Token(TokenType.SEPARATOR, Character.toString(curChar), cursor));
			} else if (!Character.isWhitespace(curChar)) {
				// parse literal
				final StringBuilder literal = new StringBuilder();
				boolean isEscaped = false;
				while (cursor < chars.length && (!isDelimiter(chars[cursor]) || isEscaped)) {
					curChar = chars[cursor++];
					if (!isEscaped && curChar == '`') {
						isEscaped = true;
					} else if (isEscaped && curChar == '`' && cursor < chars.length && chars[cursor] == '`') {
						// escaped backtick using "`Hello `` World`"
						cursor++;
						literal.append(curChar);
					} else if (isEscaped && curChar == '`') {
						break;
					} else {
						literal.append(curChar);
					}
				}
				cursor -= 1;
				tokens.add(new Token(TokenType.LITERAL, literal.toString(), cursor));
			}
		}

		return tokens;
	}

	// --------------------------------------------------------------------------------------------

	private enum TokenType {
		LITERAL, BEGIN, END, SEPARATOR
	}

	private static class Token {
		public final TokenType type;
		public final String literal;
		public final int cursorPosition;

		public Token(TokenType type, String literal, int cursorPosition) {
			this.type = type;
			this.literal = literal;
			this.cursorPosition = cursorPosition;
		}
	}

	private static class TokenConverter {

		private String inputString;
		private List<Token> tokens;
		private int lastValidToken;
		private int currentToken;

		public TokenConverter(String inputString, List<Token> tokens) {
			this.inputString = inputString;
			this.tokens = tokens;
			this.lastValidToken = -1;
			this.currentToken = -1;
		}

		public TypeInformation<?> convert() {
			nextToken(TokenType.LITERAL);
			final TypeInformation<?> typeInfo = convertType();
			if (hasRemainingTokens()) {
				nextToken();
				throw parsingError("Unexpected token: " + token().literal);
			}
			return typeInfo;
		}

		public InternalType convert2() {
			nextToken(TokenType.LITERAL);
			final InternalType dt = convertDataType();
			if (hasRemainingTokens()) {
				nextToken();
				throw parsingError("Unexpected token: " + token().literal);
			}
			return dt;
		}

		private TypeInformation<?> convertType() {
			final TypeInformation<?> typeInfo;
			switch (token().literal) {
				case VARCHAR:
				case STRING:
					return Types.STRING;
				case BOOLEAN:
					return Types.BOOLEAN;
				case TINYINT:
				case BYTE:
					return Types.BYTE;
				case SMALLINT:
				case SHORT:
					return Types.SHORT;
				case INT:
					return Types.INT;
				case BIGINT:
				case LONG:
					return Types.LONG;
				case FLOAT:
					return Types.FLOAT;
				case DOUBLE:
					return Types.DOUBLE;
				case DECIMAL:
					return Types.BIG_DEC;
				case DATE:
				case SQL_DATE:
					return Types.SQL_DATE;
				case TIMESTAMP:
				case SQL_TIMESTAMP:
					return Types.SQL_TIMESTAMP;
				case TIME:
				case SQL_TIME:
					return Types.SQL_TIME;
				case ROW:
					return convertRow();
				case ANY:
					return convertAny();
				case POJO:
					return convertPojo();
				case MAP:
					return convertMap();
				case MULTISET:
					return convertMultiset();
				case PRIMITIVE_ARRAY:
					return convertPrimitiveArray();
				case OBJECT_ARRAY:
					return convertObjectArray();
				default:
					throw parsingError("Unsupported type: " + token().literal);
			}
		}

		private InternalType convertDataType() {
			switch (token().literal) {
				case VARCHAR:
				case STRING:
					return org.apache.flink.table.types.Types.STRING;
				case BOOLEAN:
					return org.apache.flink.table.types.Types.BOOLEAN;
				case TINYINT:
				case BYTE:
					return org.apache.flink.table.types.Types.BYTE;
				case SMALLINT:
				case SHORT:
					return org.apache.flink.table.types.Types.SHORT;
				case INT:
					return org.apache.flink.table.types.Types.INT;
				case BIGINT:
				case LONG:
					return org.apache.flink.table.types.Types.LONG;
				case FLOAT:
					return org.apache.flink.table.types.Types.FLOAT;
				case DOUBLE:
					return org.apache.flink.table.types.Types.DOUBLE;
				case DECIMAL:
					return org.apache.flink.table.types.Types.DECIMAL;
				case DATE:
				case SQL_DATE:
					return org.apache.flink.table.types.Types.DATE;
				case TIME:
				case SQL_TIME:
					return org.apache.flink.table.types.Types.TIME;
				case TIMESTAMP:
				case SQL_TIMESTAMP:
					return org.apache.flink.table.types.Types.TIMESTAMP;
				case ROW:
					return convertDataTypeRow();
				case ANY:
					return convertDataTypeAny();
				case MAP:
					return convertDataTypeMap();
				case MULTISET:
					return convertDataTypeMultiset();
				case PRIMITIVE_ARRAY:
					return convertDataTypePrimitiveArray();
				case OBJECT_ARRAY:
					return convertDataTypeObjectArray();
				default:
					throw parsingError("Unsupported type: " + token().literal);

			}
		}

		private InternalType convertDataTypeRow() {
			nextToken(TokenType.BEGIN);

			// check if ROW<INT, INT> or ROW<name INT, other INT>
			if (isNextToken(2, TokenType.LITERAL)) {
				// named row
				final List<String> names = new ArrayList<>();
				final List<InternalType> types = new ArrayList<>();
				while (hasRemainingTokens()) {
					nextToken(TokenType.LITERAL);
					names.add(token().literal);

					nextToken(TokenType.LITERAL);
					types.add(convertDataType());

					if (isNextToken(1, TokenType.END)) {
						break;
					}
					nextToken(TokenType.SEPARATOR);
				}
				nextToken(TokenType.END);

				return new RowType(
					types.toArray(new InternalType[0]),
					names.toArray(new String[0])
				);
			} else {
				// unnamed row
				final List<InternalType> types = new ArrayList<>();
				while (hasRemainingTokens()) {
					nextToken(TokenType.LITERAL);
					types.add(convertDataType());

					if (isNextToken(1, TokenType.END)) {
						break;
					}
					nextToken(TokenType.SEPARATOR);
				}
				nextToken(TokenType.END);
				return new RowType(types.toArray(new InternalType[0]));
			}
		}

		private InternalType convertDataTypeAny() {
			nextToken(TokenType.BEGIN);

			// we only support ANY(class)
			nextToken(TokenType.LITERAL);
			final String clazzName = token().literal;

			nextToken(TokenType.END);

			final Class<?> clazz = EncodingUtils.loadClass(clazzName);
			return new GenericType<>(clazz);
		}

		private InternalType convertDataTypeMap() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final InternalType keyType = convertDataType();

			nextToken(TokenType.SEPARATOR);

			nextToken(TokenType.LITERAL);
			final InternalType valueType = convertDataType();

			nextToken(TokenType.END);

			return new MapType(keyType, valueType);
		}

		private InternalType convertDataTypeMultiset() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final InternalType elementType = convertDataType();

			nextToken(TokenType.END);
			return new MultisetType(elementType);
		}

		private InternalType convertDataTypePrimitiveArray() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final InternalType elementType = convertDataType();

			nextToken(TokenType.END);
			return new ArrayType(elementType, true);
		}

		private InternalType convertDataTypeObjectArray() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final InternalType elementType = convertDataType();

			nextToken(TokenType.END);
			return new ArrayType(elementType, false);
		}

		private TypeInformation<?> convertRow() {
			nextToken(TokenType.BEGIN);

			// check if ROW<INT, INT> or ROW<name INT, other INT>
			if (isNextToken(2, TokenType.LITERAL)) {
				// named row
				final List<String> names = new ArrayList<>();
				final List<TypeInformation<?>> types = new ArrayList<>();
				while (hasRemainingTokens()) {
					nextToken(TokenType.LITERAL);
					names.add(token().literal);

					nextToken(TokenType.LITERAL);
					types.add(convertType());

					if (isNextToken(1, TokenType.END)) {
						break;
					}
					nextToken(TokenType.SEPARATOR);
				}

				nextToken(TokenType.END);

				return Types.ROW_NAMED(
					names.toArray(new String[0]),
					types.toArray(new TypeInformation<?>[0]));
			} else {
				// unnamed row
				final List<TypeInformation<?>> types = new ArrayList<>();
				while (hasRemainingTokens()) {
					nextToken(TokenType.LITERAL);
					types.add(convertType());

					if (isNextToken(1, TokenType.END)) {
						break;
					}
					nextToken(TokenType.SEPARATOR);
				}

				nextToken(TokenType.END);

				return Types.ROW(types.toArray(new TypeInformation<?>[0]));
			}
		}

		private TypeInformation<?> convertAny() {
			nextToken(TokenType.BEGIN);

			// check if ANY(class) or ANY(class, serialized)
			if (isNextToken(2, TokenType.SEPARATOR)) {
				// any type information
				nextToken(TokenType.LITERAL);
				final String className = token().literal;

				nextToken(TokenType.SEPARATOR);

				nextToken(TokenType.LITERAL);
				final String serialized = token().literal;

				nextToken(TokenType.END);

				final Class<?> clazz = EncodingUtils.loadClass(className);
				final TypeInformation<?> typeInfo = EncodingUtils.decodeStringToObject(serialized, TypeInformation.class);
				if (!clazz.equals(typeInfo.getTypeClass())) {
					throw new ValidationException("Class '" + clazz + "' does no correspond to serialized data.");
				}
				return typeInfo;
			} else {
				// generic type information
				nextToken(TokenType.LITERAL);
				final String className = token().literal;

				nextToken(TokenType.END);

				final Class<?> clazz = EncodingUtils.loadClass(className);
				return Types.GENERIC(clazz);
			}
		}

		private TypeInformation<?> convertPojo() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final String className = token().literal;

			nextToken(TokenType.END);

			final Class<?> clazz = EncodingUtils.loadClass(className);
			return Types.POJO(clazz);
		}

		private TypeInformation<?> convertMap() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final TypeInformation<?> keyTypeInfo = convertType();

			nextToken(TokenType.SEPARATOR);

			nextToken(TokenType.LITERAL);
			final TypeInformation<?> valueTypeInfo = convertType();

			nextToken(TokenType.END);

			return Types.MAP(keyTypeInfo, valueTypeInfo);
		}

		private TypeInformation<?> convertMultiset() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final TypeInformation<?> elementTypeInfo = convertType();

			nextToken(TokenType.END);

			return new MultisetTypeInfo<>(elementTypeInfo);
		}

		private TypeInformation<?> convertPrimitiveArray() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final TypeInformation<?> elementTypeInfo = convertType();

			nextToken(TokenType.END);

			return Types.PRIMITIVE_ARRAY(elementTypeInfo);
		}

		private TypeInformation<?> convertObjectArray() {
			nextToken(TokenType.BEGIN);

			nextToken(TokenType.LITERAL);
			final TypeInformation<?> elementTypeInfo = convertType();

			nextToken(TokenType.END);

			return Types.OBJECT_ARRAY(elementTypeInfo);
		}

		private void nextToken(TokenType type) {
			nextToken();
			final Token nextToken = tokens.get(currentToken);
			if (nextToken.type != type) {
				throw parsingError(type.name() + " expected but was " + nextToken.type + '.');
			}
		}

		private void nextToken() {
			this.currentToken++;
			if (currentToken >= tokens.size()) {
				throw parsingError("Unexpected end.");
			}
			lastValidToken = this.currentToken - 1;
		}

		private boolean isNextToken(int lookAhead, TokenType type) {
			return currentToken + lookAhead < tokens.size() &&
				tokens.get(currentToken + lookAhead).type == type;
		}

		private int lastCursor() {
			if (lastValidToken < 0) {
				return 0;
			}
			return tokens.get(lastValidToken).cursorPosition + 1;
		}

		private ValidationException parsingError(String cause) {
			throw new ValidationException("Could not parse type information at position " + lastCursor() + ": " + cause + "\n" +
				"Input type string: " + inputString);
		}

		private Token token() {
			return tokens.get(currentToken);
		}

		private boolean hasRemainingTokens() {
			return currentToken + 1 < tokens.size();
		}
	}
}
