// Delta Lake transaction-log action models and their JSON encoding.
// One JSON object per line in `_delta_log/NNNNNNNNNNNNNNNNNNNN.json`.

/// `protocol` action.
class Protocol {
  final int minReaderVersion;
  final int minWriterVersion;
  final List<String>? readerFeatures;
  final List<String>? writerFeatures;

  Protocol(
    this.minReaderVersion,
    this.minWriterVersion, {
    this.readerFeatures,
    this.writerFeatures,
  });

  factory Protocol.fromJson(Map<String, dynamic> json) => Protocol(
    json['minReaderVersion'] as int,
    json['minWriterVersion'] as int,
    readerFeatures: (json['readerFeatures'] as List?)?.cast<String>(),
    writerFeatures: (json['writerFeatures'] as List?)?.cast<String>(),
  );

  /// Reader features this library understands (under the table-features protocol,
  /// minReaderVersion 3). Anything outside this set, or a legacy reader version
  /// implying an unsupported capability (v2 = column mapping): is rejected.
  static const supportedReaderFeatures = {'deletionVectors'};

  /// Writer features this library understands (table-features, minWriterVersion 7).
  static const supportedWriterFeatures = {
    'appendOnly',
    'invariants',
    'deletionVectors',
  };

  /// `null` if this library can safely READ a table with this protocol, else a
  /// human-readable reason it cannot. Fail-closed: an unknown reader feature or a
  /// reader version implying an unsupported capability (e.g. column mapping)
  /// would otherwise be read with ordinary name/Parquet semantics: plausible but
  /// WRONG data.
  String? readerUnsupported() {
    if (minReaderVersion <= 1) return null; // basic Parquet: fully supported
    if (minReaderVersion == 3) {
      // Table-features protocol: every listed reader feature must be recognized.
      for (final feature in readerFeatures ?? const <String>[]) {
        if (!supportedReaderFeatures.contains(feature)) {
          return 'unsupported reader feature "$feature"';
        }
      }
      return null;
    }
    // v2 implies column mapping; v>=4 is newer than we understand.
    return 'unsupported minReaderVersion $minReaderVersion';
  }

  /// `null` if this library can safely WRITE (mutate) a table with this protocol,
  /// else why not. Mutating a table whose writer features we don't enforce (CHECK
  /// constraints, generated columns, column mapping, …) could violate its
  /// invariants: fail closed.
  String? writerUnsupported() {
    if (minWriterVersion <= 2) return null; // basic + appendOnly/invariants
    if (minWriterVersion == 7) {
      for (final feature in writerFeatures ?? const <String>[]) {
        if (!supportedWriterFeatures.contains(feature)) {
          return 'unsupported writer feature "$feature"';
        }
      }
      return null;
    }
    // v3..6 imply CHECK constraints / CDF / generated columns / column mapping.
    return 'unsupported minWriterVersion $minWriterVersion';
  }

  Map<String, dynamic> toJson() => {
    'protocol': {
      'minReaderVersion': minReaderVersion,
      'minWriterVersion': minWriterVersion,
      if (readerFeatures != null) 'readerFeatures': readerFeatures,
      if (writerFeatures != null) 'writerFeatures': writerFeatures,
    },
  };
}

/// `metaData` action.
class Metadata {
  final String id;
  final String? name;
  final String schemaString;
  final List<String> partitionColumns;
  final Map<String, String> configuration;
  final int? createdTime;

  Metadata({
    required this.id,
    this.name,
    required this.schemaString,
    this.partitionColumns = const [],
    this.configuration = const {},
    this.createdTime,
  });

  factory Metadata.fromJson(Map<String, dynamic> json) => Metadata(
    id: json['id'] as String,
    name: json['name'] as String?,
    schemaString: json['schemaString'] as String,
    partitionColumns:
        (json['partitionColumns'] as List?)?.cast<String>() ?? const [],
    configuration:
        (json['configuration'] as Map?)?.cast<String, String>() ?? const {},
    createdTime: json['createdTime'] as int?,
  );

  Map<String, dynamic> toJson() => {
    'metaData': {
      'id': id,
      if (name != null) 'name': name,
      'format': {'provider': 'parquet', 'options': <String, String>{}},
      'schemaString': schemaString,
      'partitionColumns': partitionColumns,
      'configuration': configuration,
      if (createdTime != null) 'createdTime': createdTime,
    },
  };
}

/// Deletion-vector descriptor embedded in `add`/`remove`.
class DeletionVectorDescriptor {
  /// 'u' = UUID relative path, 'i' = inline (z85), 'p' = absolute path.
  final String storageType;
  final String pathOrInlineDv;
  final int? offset;
  final int sizeInBytes;
  final int cardinality;

  DeletionVectorDescriptor({
    required this.storageType,
    required this.pathOrInlineDv,
    this.offset,
    required this.sizeInBytes,
    required this.cardinality,
  });

  factory DeletionVectorDescriptor.fromJson(Map<String, dynamic> json) =>
      DeletionVectorDescriptor(
        storageType: json['storageType'] as String,
        pathOrInlineDv: json['pathOrInlineDv'] as String,
        offset: json['offset'] as int?,
        sizeInBytes: json['sizeInBytes'] as int,
        cardinality: json['cardinality'] as int,
      );

  Map<String, dynamic> toJson() => {
    'storageType': storageType,
    'pathOrInlineDv': pathOrInlineDv,
    if (offset != null) 'offset': offset,
    'sizeInBytes': sizeInBytes,
    'cardinality': cardinality,
  };
}

/// `add` action.
class AddFile {
  final String path;
  final Map<String, String> partitionValues;
  final int size;
  final int modificationTime;
  final bool dataChange;
  final String? stats;
  final DeletionVectorDescriptor? deletionVector;

  AddFile({
    required this.path,
    this.partitionValues = const {},
    this.size = 0,
    this.modificationTime = 0,
    this.dataChange = true,
    this.stats,
    this.deletionVector,
  });

  factory AddFile.fromJson(Map<String, dynamic> json) => AddFile(
    path: json['path'] as String,
    partitionValues:
        (json['partitionValues'] as Map?)?.cast<String, String>() ?? const {},
    size: (json['size'] as num?)?.toInt() ?? 0,
    modificationTime: (json['modificationTime'] as num?)?.toInt() ?? 0,
    dataChange: json['dataChange'] as bool? ?? true,
    stats: json['stats'] as String?,
    deletionVector: json['deletionVector'] == null
        ? null
        : DeletionVectorDescriptor.fromJson(
            (json['deletionVector'] as Map).cast<String, dynamic>(),
          ),
  );

  Map<String, dynamic> toJson() => {
    'add': {
      'path': path,
      'partitionValues': partitionValues,
      'size': size,
      'modificationTime': modificationTime,
      'dataChange': dataChange,
      if (stats != null) 'stats': stats,
      if (deletionVector != null) 'deletionVector': deletionVector!.toJson(),
    },
  };

  /// A key uniquely identifying the (file, DV) pair in a snapshot.
  String get key => deletionVector == null
      ? path
      : '$path|${deletionVector!.storageType}${deletionVector!.pathOrInlineDv}'
            '@${deletionVector!.offset}';

  AddFile withDeletionVector(DeletionVectorDescriptor? vector) => AddFile(
    path: path,
    partitionValues: partitionValues,
    size: size,
    modificationTime: modificationTime,
    dataChange: dataChange,
    stats: stats,
    deletionVector: vector,
  );
}

/// `remove` action.
class RemoveFile {
  final String path;
  final int? deletionTimestamp;
  final bool dataChange;
  final DeletionVectorDescriptor? deletionVector;

  RemoveFile({
    required this.path,
    this.deletionTimestamp,
    this.dataChange = true,
    this.deletionVector,
  });

  factory RemoveFile.fromJson(Map<String, dynamic> json) => RemoveFile(
    path: json['path'] as String,
    deletionTimestamp: (json['deletionTimestamp'] as num?)?.toInt(),
    dataChange: json['dataChange'] as bool? ?? true,
    deletionVector: json['deletionVector'] == null
        ? null
        : DeletionVectorDescriptor.fromJson(
            (json['deletionVector'] as Map).cast<String, dynamic>(),
          ),
  );

  Map<String, dynamic> toJson() => {
    'remove': {
      'path': path,
      if (deletionTimestamp != null) 'deletionTimestamp': deletionTimestamp,
      'dataChange': dataChange,
      if (deletionVector != null) 'deletionVector': deletionVector!.toJson(),
    },
  };

  String get key => deletionVector == null
      ? path
      : '$path|${deletionVector!.storageType}${deletionVector!.pathOrInlineDv}'
            '@${deletionVector!.offset}';
}
