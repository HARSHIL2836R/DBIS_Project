import pyarrow.parquet as pq
from collections import defaultdict

def extract_index_coordinates(file_path: str, target_col_idx: int) -> list:
    grouped_data = defaultdict(set)
    
    try:
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema
        
        if target_col_idx < 0 or target_col_idx >= len(schema.names):
            raise ValueError(f"Column index {target_col_idx} is out of bounds.")
            
        target_col_name = schema.names[target_col_idx]
        num_row_groups = parquet_file.metadata.num_row_groups

        for row_group_id in range(num_row_groups):
            table = parquet_file.read_row_group(row_group_id, columns=[target_col_name])
            column_data = table.column(0)
            
            for val in column_data:
                if val.is_valid:
                    native_val = val.as_py()
                    
                    if isinstance(native_val, bool):
                        str_val = "true" if native_val else "false"
                    elif isinstance(native_val, bytes):
                        str_val = native_val.decode('utf-8')
                    else:
                        str_val = str(native_val)
                        
                    # Add the row group ID to this value's set
                    grouped_data[str_val].add(row_group_id)

        # Convert the dictionary into your exact requested JSON-like list
        # We sort the set into a list so the row group IDs are in order: [0, 2]
        records = [
            {"value": val, "file_path": file_path, "rowgroup_ids": sorted(list(rg_ids))}
            for val, rg_ids in grouped_data.items()
        ]
        
        return records

    except Exception as e:
        print(f"Parquet Extraction Error: {e}")
        return []


#manually checking

# if __name__ == "__main__":
#     #take file path and column index as input
#     import argparse
#     parser = argparse.ArgumentParser(description="Extract index coordinates from a Parquet file.")
#     parser.add_argument("file_path", type=str, help="Path to the Parquet file")
#     parser.add_argument("column_index", type=int, help="Column index to extract")
#     args = parser.parse_args()

#     #call the function and print the results
#     extracted_records = extract_index_coordinates(args.file_path, args.column_index)
#     print(f"Extracted {len(extracted_records)} records:")
#     for record in extracted_records[:10]:  # Print only the first 10 records for brevity
#         print(record)