// Helper process for the Round-50 #7 cross-process file-lock CAS test: appends a
// single row to the Delta table at args[0] with value args[1], using the
// FileLockDeltaCommitCoordinator. Run as a SEPARATE OS process (fcntl locks are
// per-process), several concurrently, to prove no commit is lost.
import 'package:libbigdata/libbigdata.dart';

Future<void> main(List<String> args) async {
  final path = args[0];
  final id = int.parse(args[1]);
  final writer = DeltaWriter(
    path,
    coordinator: const FileLockDeltaCommitCoordinator(),
  );
  await writer.write(
    Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [id], nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    ),
  );
}
