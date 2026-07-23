import 'package:meta/meta.dart';

/// Represents the data types supported by the bigdata library
@immutable
class DataType {
  final String name;
  final Type dartType;
  final int? fixedSize;
  final bool isNumeric;
  final bool isTemporal;
  final bool isNested;

  const DataType._({
    required this.name,
    required this.dartType,
    this.fixedSize,
    this.isNumeric = false,
    this.isTemporal = false,
    this.isNested = false,
  });

  /// An integer type (int8/16/32/64) — numeric and stored as a Dart `int`, as
  /// opposed to the `double`-backed float/decimal numerics.
  bool get isInteger => isNumeric && dartType == int;

  static const DataType boolean = DataType._(
    name: 'boolean',
    dartType: bool,
    fixedSize: 1,
  );

  static const DataType int8 = DataType._(
    name: 'int8',
    dartType: int,
    fixedSize: 1,
    isNumeric: true,
  );

  static const DataType int16 = DataType._(
    name: 'int16',
    dartType: int,
    fixedSize: 2,
    isNumeric: true,
  );

  static const DataType int32 = DataType._(
    name: 'int32',
    dartType: int,
    fixedSize: 4,
    isNumeric: true,
  );

  static const DataType int64 = DataType._(
    name: 'int64',
    dartType: int,
    fixedSize: 8,
    isNumeric: true,
  );

  static const DataType float32 = DataType._(
    name: 'float32',
    dartType: double,
    fixedSize: 4,
    isNumeric: true,
  );

  static const DataType float64 = DataType._(
    name: 'float64',
    dartType: double,
    fixedSize: 8,
    isNumeric: true,
  );

  static const DataType string = DataType._(name: 'string', dartType: String);

  static const DataType binary = DataType._(
    name: 'binary',
    dartType: List<int>,
  );

  static const DataType date = DataType._(
    name: 'date',
    dartType: DateTime,
    fixedSize: 4,
    isTemporal: true,
  );

  static const DataType timestamp = DataType._(
    name: 'timestamp',
    dartType: DateTime,
    fixedSize: 8,
    isTemporal: true,
  );

  static const DataType decimal = DataType._(
    name: 'decimal',
    dartType: double,
    fixedSize: 16,
    isNumeric: true,
  );

  static const DataType list = DataType._(
    name: 'list',
    dartType: List,
    isNested: true,
  );

  static const DataType map = DataType._(
    name: 'map',
    dartType: Map,
    isNested: true,
  );

  static const DataType struct = DataType._(
    name: 'struct',
    dartType: Map,
    isNested: true,
  );

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
      const numericHierarchy = [
        int8,
        int16,
        int32,
        int64,
        float32,
        float64,
        decimal,
      ];
      final aIndex = numericHierarchy.indexOf(a);
      final bIndex = numericHierarchy.indexOf(b);
      if (aIndex >= 0 && bIndex >= 0) {
        return numericHierarchy[aIndex > bIndex ? aIndex : bIndex];
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
    bool isInt(DataType t) =>
        t == int8 || t == int16 || t == int32 || t == int64;
    bool isFloat(DataType t) => t == float32 || t == float64;
    if ((isInt(a) && isFloat(b)) || (isFloat(a) && isInt(b))) return decimal;
    return commonType(a, b);
  }
}
