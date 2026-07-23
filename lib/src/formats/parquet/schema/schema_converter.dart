import '../../../core/conversions.dart';
import '../../../core/schema.dart' as core;
import '../../../core/data_type.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import 'schema_levels.dart';

/// Physical Parquet representation of a DECIMAL of the given [precision]:
/// INT32 (≤9 digits), INT64 (≤18), else FIXED_LEN_BYTE_ARRAY sized to hold the
/// unscaled two's-complement value.
({thrift.ParquetType type, int? typeLength}) decimalLayout(int precision) {
  if (precision <= 9) return (type: thrift.ParquetType.int32, typeLength: null);
  if (precision <= 18) {
    return (type: thrift.ParquetType.int64, typeLength: null);
  }
  final bits = (precision * 3.3219).ceil() + 1;
  return (
    type: thrift.ParquetType.fixedLenByteArray,
    typeLength: (bits + 7) ~/ 8,
  );
}

/// Convert a logical Dart value to its Parquet physical representation:
/// date→epoch days (int), timestamp→epoch micros (int), decimal→unscaled int
/// (INT32/INT64) or big-endian two's-complement bytes (FLBA). Other types pass
/// through unchanged.
dynamic toPhysicalValue(DataType type, int precision, dynamic v) {
  if (v == null) return null;
  if (type == DataType.date) return dateToEpochDays(v as DateTime);
  if (type == DataType.timestamp) return timestampToEpochMicros(v as DateTime);
  if (type == DataType.decimal) {
    final unscaled = v as BigInt;
    final layout = decimalLayout(precision);
    if (layout.type == thrift.ParquetType.int32 ||
        layout.type == thrift.ParquetType.int64) {
      return unscaled.toInt();
    }
    return bigIntToBytes(unscaled, layout.typeLength!, littleEndian: false);
  }
  return v;
}

/// Converts between Parquet schema and internal schema representations
class SchemaConverter {
  /// Convert Parquet schema to internal schema
  ParquetSchema fromParquetSchema(List<thrift.SchemaElement> elements) {
    final fields = <ParquetSchemaField>[];

    // Name each leaf by its full dotted path so nested fields that share a leaf
    // name (e.g. "element", "key") stay unique.
    final paths = SchemaLevels.leafPaths(elements);
    int leaf = 0;
    for (int i = 1; i < elements.length; i++) {
      final element = elements[i];
      if (element.type != null) {
        // Primitive elements have a type
        final path = leaf < paths.length ? paths[leaf] : element.name!;
        final dataType = convertType(element);
        // uint64 reads as decimal(20,0); other decimals carry the file's params.
        final unsigned = unsignedInfo(element);
        fields.add(
          ParquetSchemaField(
            name: path,
            dataType: dataType,
            // Nested (dotted) columns are reconstructed as lists, so nullable.
            nullable: isNullable(element) || path.contains('.'),
            precision: dataType == DataType.decimal
                ? (unsigned?.precision ??
                      element.logicalType?.decimal?.precision ??
                      element.precision)
                : null,
            scale: dataType == DataType.decimal
                ? (unsigned?.scale ??
                      element.logicalType?.decimal?.scale ??
                      element.scale)
                : null,
            id: element.fieldId,
          ),
        );
        leaf++;
      }
    }

    return ParquetSchema(elements: elements, fields: fields);
  }

  /// Convert internal schema to Parquet schema
  List<thrift.SchemaElement> toParquetSchema(core.Schema schema) {
    final elements = <thrift.SchemaElement>[];

    // Add root element
    final root = thrift.SchemaElement();
    root.name = 'schema';
    root.numChildren = schema.fields.length;
    elements.add(root);

    // Add field elements (nested types expand into group + child elements).
    for (final field in schema.fields) {
      _emitField(field, elements);
    }

    return elements;
  }

  /// Append [field]'s schema element(s) (recursing for nested types).
  void _emitField(core.Field field, List<thrift.SchemaElement> out) {
    final repetition = field.nullable
        ? thrift.FieldRepetitionType.optional
        : thrift.FieldRepetitionType.required;

    if (field.dataType == DataType.struct) {
      out.add(
        thrift.SchemaElement()
          ..name = field.name
          ..repetitionType = repetition
          ..fieldId = field
              .id // Iceberg field-id on the struct group
          ..numChildren = field.children.length,
      );
      for (final c in field.children) {
        _emitField(c, out);
      }
      return;
    }
    if (field.dataType == DataType.list) {
      // Standard 3-level LIST: <field> (LIST) -> list (repeated) -> element.
      // Iceberg field-ids sit on the LIST group and the 'element' (not the
      // synthetic 'list' repeated level).
      final element = field.children.first;
      out.add(
        thrift.SchemaElement()
          ..name = field.name
          ..repetitionType = repetition
          ..convertedType = thrift.ConvertedType.list
          ..fieldId = field.id
          ..numChildren = 1,
      );
      out.add(
        thrift.SchemaElement()
          ..name = 'list'
          ..repetitionType = thrift.FieldRepetitionType.repeated
          ..numChildren = 1,
      );
      _emitField(element.copyWith(name: 'element'), out);
      return;
    }
    if (field.dataType == DataType.map) {
      final entries = field.children.first; // key_value struct(key,value)
      out.add(
        thrift.SchemaElement()
          ..name = field.name
          ..repetitionType = repetition
          ..convertedType = thrift.ConvertedType.map
          ..fieldId = field.id
          ..numChildren = 1,
      );
      out.add(
        thrift.SchemaElement()
          ..name = 'key_value'
          ..repetitionType = thrift.FieldRepetitionType.repeated
          ..numChildren = 2,
      );
      _emitField(entries.children[0].copyWith(name: 'key'), out);
      _emitField(entries.children[1].copyWith(name: 'value'), out);
      return;
    }

    out.add(scalarElement(field));
  }

  /// The Parquet [SchemaElement] for a scalar [field] (physical type + legacy
  /// converted-type annotation + decimal scale/precision/typeLength).
  thrift.SchemaElement scalarElement(core.Field field) {
    final element = thrift.SchemaElement()
      ..name = field.name
      ..repetitionType = field.nullable
          ? thrift.FieldRepetitionType.optional
          : thrift.FieldRepetitionType.required;
    // Emit the column field-id when set (Iceberg/Spark-Iceberg use file-level
    // field-ids for schema evolution / column reordering).
    if (field.id != null) element.fieldId = field.id;
    if (field.dataType == DataType.decimal) {
      final precision = field.precision ?? 38;
      final layout = decimalLayout(precision);
      element.type = layout.type;
      if (layout.typeLength != null) element.typeLength = layout.typeLength;
      element.scale = field.scale ?? 0;
      element.precision = precision;
      element.convertedType = thrift.ConvertedType.decimal;
    } else {
      element.type = toParquetType(field.dataType);
      element.convertedType = switch (field.dataType) {
        DataType.string => thrift.ConvertedType.utf8,
        DataType.date => thrift.ConvertedType.date,
        DataType.timestamp => thrift.ConvertedType.timestampMicros,
        // int8/int16 store as INT32 physically; annotate the narrow width so a
        // reader (incl. our own spill round-trip) recovers int8/int16 instead of
        // widening to int32.
        DataType.int8 => thrift.ConvertedType.int8,
        DataType.int16 => thrift.ConvertedType.int16,
        _ => null,
      };
    }
    return element;
  }

  /// Convert Parquet type to internal data type
  DataType convertType(thrift.SchemaElement element) {
    // Unsigned integers (either annotation) have a single source of truth.
    final unsigned = unsignedInfo(element);
    if (unsigned != null) return unsigned.type;

    // Check logical type first
    if (element.logicalType != null) {
      final logical = element.logicalType!;
      if (logical.string != null) return DataType.string;
      if (logical.date != null) return DataType.date;
      if (logical.timestamp != null) return DataType.timestamp;
      if (logical.decimal != null) return DataType.decimal;
      if (logical.integer != null) {
        return _intType(logical.integer!.bitWidth ?? 32, true);
      }
    }

    // Check converted type
    if (element.convertedType != null) {
      switch (element.convertedType!) {
        case thrift.ConvertedType.utf8:
          return DataType.string;
        case thrift.ConvertedType.date:
          return DataType.date;
        case thrift.ConvertedType.timestampMillis:
        case thrift.ConvertedType.timestampMicros:
          return DataType.timestamp;
        case thrift.ConvertedType.decimal:
          return DataType.decimal;
        case thrift.ConvertedType.int8:
          return DataType.int8;
        case thrift.ConvertedType.int16:
          return DataType.int16;
        default:
          break;
      }
    }

    // Fall back to physical type
    switch (element.type!) {
      case thrift.ParquetType.boolean:
        return DataType.boolean;
      case thrift.ParquetType.int32:
        return DataType.int32;
      case thrift.ParquetType.int64:
        return DataType.int64;
      case thrift.ParquetType.int96:
        return DataType.timestamp;
      case thrift.ParquetType.float:
        return DataType.float32;
      case thrift.ParquetType.double:
        return DataType.float64;
      case thrift.ParquetType.byteArray:
      case thrift.ParquetType.fixedLenByteArray:
        // An unannotated byte array is binary; UTF8/String-annotated columns
        // are resolved to string above (convertedType/logicalType checks).
        return DataType.binary;
    }
  }

  /// Map a SIGNED integer bit-width to its libbigdata type.
  static DataType _intType(int bits, bool signed) => switch (bits) {
    8 => DataType.int8,
    16 => DataType.int16,
    32 => DataType.int32,
    _ => DataType.int64,
  };

  /// The single source of truth for an UNSIGNED Parquet integer column (via
  /// either the modern `logicalType.integer{isSigned:false}` or the legacy
  /// `convertedType.uint*`). Returns its bit-width, the libbigdata type it reads
  /// as, and decimal precision/scale when applicable — null for signed columns.
  ///
  /// uint8/16/32 widen to the next signed type that holds the full range
  /// (int16/int32/int64), so e.g. uint32 4000000000 is not read as a negative.
  /// uint64 exceeds int64, so it reads as `decimal(20,0)` (BigInt-backed, the
  /// same mapping Spark uses) — lossless, never wrapping. Drives [convertType],
  /// [fromParquetSchema] precision/scale, and the reader's reinterpretation.
  static ({int bits, DataType type, int? precision, int? scale})? unsignedInfo(
    thrift.SchemaElement element,
  ) {
    final logical = element.logicalType?.integer;
    int? bits;
    if (logical != null && logical.isSigned == false) {
      bits = logical.bitWidth ?? 32;
    } else {
      bits = switch (element.convertedType) {
        thrift.ConvertedType.uint8 => 8,
        thrift.ConvertedType.uint16 => 16,
        thrift.ConvertedType.uint32 => 32,
        thrift.ConvertedType.uint64 => 64,
        _ => null,
      };
    }
    return switch (bits) {
      8 => (bits: 8, type: DataType.int16, precision: null, scale: null),
      16 => (bits: 16, type: DataType.int32, precision: null, scale: null),
      32 => (bits: 32, type: DataType.int64, precision: null, scale: null),
      64 => (bits: 64, type: DataType.decimal, precision: 20, scale: 0),
      _ => null,
    };
  }

  /// Reinterpret a signed physical integer [v] (read as two's-complement) as the
  /// unsigned value of the given [bits]. uint8/16/32 stay `int` (lossless in
  /// int64); the constant modulus avoids `1 << 32` wrapping to 1 under JS.
  /// uint64 cannot fit int64, so it returns a [BigInt] (full range, no loss).
  static Object reinterpretUnsigned(int v, int bits) {
    if (bits >= 64) return BigInt.from(v).toUnsigned(64);
    if (v >= 0) return v;
    return v + const {8: 0x100, 16: 0x10000, 32: 0x100000000}[bits]!;
  }

  /// Check if element is nullable
  bool isNullable(thrift.SchemaElement element) {
    return element.repetitionType != thrift.FieldRepetitionType.required;
  }

  /// Convert internal data type to Parquet type
  thrift.ParquetType toParquetType(DataType type) {
    switch (type) {
      case DataType.boolean:
        return thrift.ParquetType.boolean;
      case DataType.int8:
      case DataType.int16:
      case DataType.int32:
        return thrift.ParquetType.int32;
      case DataType.int64:
        return thrift.ParquetType.int64;
      case DataType.float32:
        return thrift.ParquetType.float;
      case DataType.float64:
        return thrift.ParquetType.double;
      case DataType.string:
      case DataType.binary:
        return thrift.ParquetType.byteArray;
      case DataType.date:
        return thrift.ParquetType.int32;
      case DataType.timestamp:
        return thrift.ParquetType.int64;
      case DataType.decimal:
        return thrift.ParquetType.fixedLenByteArray;
      default:
        return thrift.ParquetType.byteArray;
    }
  }

  /// Get logical type for data type
  thrift.LogicalType? getLogicalType(DataType type) {
    switch (type) {
      case DataType.string:
        return thrift.LogicalType()..string = thrift.StringType();
      case DataType.date:
        return thrift.LogicalType()..date = thrift.DateType();
      case DataType.timestamp:
        return thrift.LogicalType()
          ..timestamp = (thrift.TimestampType()
            ..isAdjustedToUTC = true
            ..unit = (thrift.TimeUnit()..millis = thrift.MilliSeconds()));
      case DataType.decimal:
        return thrift.LogicalType()
          ..decimal = (thrift.DecimalType()
            ..scale = 2
            ..precision = 10);
      case DataType.int8:
        return thrift.LogicalType()
          ..integer = (thrift.IntType()
            ..bitWidth = 8
            ..isSigned = true);
      case DataType.int16:
        return thrift.LogicalType()
          ..integer = (thrift.IntType()
            ..bitWidth = 16
            ..isSigned = true);
      // Note: DataType doesn't have unsigned types, handled via ConvertedType
      default:
        return null;
    }
  }

  /// Get converted type for data type
  thrift.ConvertedType? getConvertedType(DataType type) {
    switch (type) {
      case DataType.string:
        return thrift.ConvertedType.utf8;
      case DataType.date:
        return thrift.ConvertedType.date;
      case DataType.timestamp:
        return thrift.ConvertedType.timestampMillis;
      case DataType.decimal:
        return thrift.ConvertedType.decimal;
      case DataType.int8:
        return thrift.ConvertedType.int8;
      case DataType.int16:
        return thrift.ConvertedType.int16;
      // Note: DataType doesn't have unsigned types
      default:
        return null;
    }
  }
}

/// Extended Parquet schema with fields
class ParquetSchema {
  final List<thrift.SchemaElement> elements;
  final List<ParquetSchemaField> fields;

  ParquetSchema({required this.elements, required this.fields});

  List<ParquetSchemaField> get leafColumns => fields;
}

/// Parquet schema field
class ParquetSchemaField {
  final String name;
  final DataType dataType;
  final bool nullable;
  final int? precision;
  final int? scale;

  /// The Parquet field-id (from the SchemaElement), when present. Iceberg/Spark
  /// use file-level field-ids so a renamed column still resolves by id — the
  /// physical name in an old file may differ from the current schema's name.
  final int? id;

  ParquetSchemaField({
    required this.name,
    required this.dataType,
    required this.nullable,
    this.precision,
    this.scale,
    this.id,
  });
}
