from collections import defaultdict

import pyarrow.parquet as pq


def _normalize_value(value):
    if isinstance(value, bytearray):
        return bytes(value)

    if isinstance(value, memoryview):
        return value.tobytes()

    return value


def extract_index_coordinates(file_path: str, target_columns: list[str]) -> dict[str, list[dict]]:
    """
    Read one parquet file and return rowgroup-level postings for each indexed column.

    Returned shape:
    {
        "age": [
            {"value": 30, "rowgroup_ids": [0, 2]},
            {"value": 45, "rowgroup_ids": [1]}
        ],
        "region": [
            {"value": "Gujarat", "rowgroup_ids": [0, 1]}
        ]
    }
    """
    if not target_columns:
        return {}

    parquet_file = pq.ParquetFile(file_path)
    schema_names = set(parquet_file.schema.names)
    missing_columns = [column for column in target_columns if column not in schema_names]

    if missing_columns:
        raise ValueError(
            f"Columns {missing_columns} are not present in parquet file {file_path}"
        )

    grouped_data: dict[str, defaultdict] = {
        column: defaultdict(set) for column in target_columns
    }

    for row_group_id in range(parquet_file.metadata.num_row_groups):
        row_group = parquet_file.read_row_group(row_group_id, columns=target_columns)

        for column_index, column_name in enumerate(row_group.column_names):
            column_data = row_group.column(column_index)

            for scalar in column_data:
                if not scalar.is_valid:
                    continue

                grouped_data[column_name][_normalize_value(scalar.as_py())].add(row_group_id)

    extracted: dict[str, list[dict]] = {}

    for column_name, values in grouped_data.items():
        extracted[column_name] = [
            {
                "value": value,
                "rowgroup_ids": sorted(rowgroup_ids),
            }
            for value, rowgroup_ids in values.items()
        ]

    return extracted
