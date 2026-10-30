import 'package:meta/meta.dart';

import 'decimal.dart';

/// Represents the data types supported by the bigdata library
@immutable
final class DataType {
  /// The type's name, as written in schemas and messages (`int64`, `string`).
  final String name;

  /// The Dart type a value of this type has (a decimal is its unscaled
  /// `BigInt`).
  final Type dartType;

  /// Bytes per value in a fixed-width layout, or null for a variable-width
  /// type.
  final int? fixedSize;

  /// Whether this is an integer, float or decimal type.
  final bool isNumeric;

  /// Whether this is [date] or [timestamp].
  final bool isTemporal;

  /// Whether this is [list], [map] or [struct].
  final bool isNested;

  const DataType._({
    required this.name,
    required this.dartType,
    this.fixedSize,
    this.isNumeric = false,
    this.isTemporal = false,
    this.isNested = false,
  });

  /// An integer type (int8/16/32/64): numeric and stored as a Dart `int`, as
  /// opposed to the `double`-backed floats and the `BigInt`-backed decimal.
  bool get isInteger => isNumeric && dartType == int;

  /// True or false.
  static const DataType boolean = DataType._(
    name: 'boolean',
    dartType: bool,
    fixedSize: 1,
  );

  /// A signed 8-bit integer.
  static const DataType int8 = DataType._(
    name: 'int8',
    dartType: int,
    fixedSize: 1,
    isNumeric: true,
  );

  /// A signed 16-bit integer.
  static const DataType int16 = DataType._(
    name: 'int16',
    dartType: int,
    fixedSize: 2,
    isNumeric: true,
  );

  /// A signed 32-bit integer.
  static const DataType int32 = DataType._(
    name: 'int32',
    dartType: int,
    fixedSize: 4,
    isNumeric: true,
  );

  /// A signed 64-bit integer.
  static const DataType int64 = DataType._(
    name: 'int64',
    dartType: int,
    fixedSize: 8,
    isNumeric: true,
  );

  /// A 32-bit IEEE 754 float.
  static const DataType float32 = DataType._(
    name: 'float32',
    dartType: double,
    fixedSize: 4,
    isNumeric: true,
  );

  /// A 64-bit IEEE 754 float.
  static const DataType float64 = DataType._(
    name: 'float64',
    dartType: double,
    fixedSize: 8,
    isNumeric: true,
  );

  /// UTF-8 text.
  static const DataType string = DataType._(name: 'string', dartType: String);

  /// Raw bytes.
  static const DataType binary = DataType._(
    name: 'binary',
    dartType: List<int>,
  );

  /// A calendar day, held as a `DateTime`.
  static const DataType date = DataType._(
    name: 'date',
    dartType: DateTime,
    fixedSize: 4,
    isTemporal: true,
  );

  /// An instant, held as a `DateTime`.
  static const DataType timestamp = DataType._(
    name: 'timestamp',
    dartType: DateTime,
    fixedSize: 8,
    isTemporal: true,
  );

  /// An exact decimal, held as an unscaled `BigInt`; its precision and scale
  /// are on the [Field] (`Field.precision`, `Field.scale`).
  static const DataType decimal = DataType._(
    name: 'decimal',
    dartType: BigInt, // unscaled; the scale lives on the Field
    fixedSize: 16,
    isNumeric: true,
  );

  /// A list of values of one element type (the field's single child).
  static const DataType list = DataType._(
    name: 'list',
    dartType: List,
    isNested: true,
  );

  /// A map from keys to values (the field's key and value children).
  static const DataType map = DataType._(
    name: 'map',
    dartType: Map,
    isNested: true,
  );

  /// A record of named fields (the field's children).
  static const DataType struct = DataType._(
    name: 'struct',
    dartType: Map,
    isNested: true,
  );

  /// The type of a column whose values are all null; it widens to the other
  /// side's type when combined with a typed column.
  static const DataType null_ = DataType._(
    name: 'null',
    dartType: Null,
    fixedSize: 0,
  );

  @override
  String toString() => name;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DataType &&
          runtimeType == other.runtimeType &&
          name == other.name;

  @override
  int get hashCode => name.hashCode;

  /// Infer data type from a Dart value
  static DataType fromValue(dynamic value) {
    if (value == null) return null_;
    if (value is bool) return boolean;
    if (value is int) return intType(value);
    if (value is double) return float64;
    if (value is String) return string;
    if (value is DateTime) return timestamp;
    // What Table.row returns for a decimal column, so rows round-trip.
    if (value is Decimal || value is BigInt) return decimal;
    if (value is List<int>) return binary;
    if (value is List) return list;
    if (value is Map) return map;
    return string;
  }

  /// Get the most appropriate integer type for a given value
  static DataType intType(int value) {
    if (value >= -128 && value <= 127) return int8;
    if (value >= -32768 && value <= 32767) return int16;
    if (value >= -2147483648 && value <= 2147483647) return int32;
    return int64;
  }

  /// Get the common type that both types can be promoted to
  static DataType? commonType(DataType a, DataType b) {
    if (a == b) return a;
    if (a == null_) return b;
    if (b == null_) return a;

    if (a.isNumeric && b.isNumeric) {
      const ladder = [int8, int16, int32, int64, float32, float64, decimal];
      final left = ladder.indexOf(a);
      final right = ladder.indexOf(b);
      if (left >= 0 && right >= 0) {
        return ladder[left > right ? left : right];
      }
    }

    if (a.isTemporal && b.isTemporal) {
      return timestamp;
    }

    if ((a == string || b == string) && !a.isNested && !b.isNested) {
      return string;
    }

    return null;
  }

  /// Like [commonType] but LOSSLESS for DATA inference: an integer mixed with a
  /// floating-point value promotes to [decimal] (which holds both exactly)
  /// instead of a float that would silently round a large integer. Used only by
  /// value inference ([Schema.infer]); SQL expression typing keeps [commonType]
  /// (there `int + 0.5 → float` is the intended, correct result).
  static DataType? inferredCommon(DataType a, DataType b) {
    bool integer(DataType t) =>
        t == int8 || t == int16 || t == int32 || t == int64;
    bool float(DataType t) => t == float32 || t == float64;
    if ((integer(a) && float(b)) || (float(a) && integer(b))) return decimal;
    return commonType(a, b);
  }
}
