import pyarrow.parquet as pq

def extract_index_coordinates(file_path: str, target_col_idx: int) -> list:
    records = []
    
    try:
        # 1. Open the Parquet file and read metadata
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema
        
        # Validate column index
        if target_col_idx < 0 or target_col_idx >= len(schema.names):
            raise ValueError(f"Column index {target_col_idx} is out of bounds.")
            
        target_col_name = schema.names[target_col_idx]
        num_row_groups = parquet_file.metadata.num_row_groups
        
        print(f"Successfully opened: {file_path}")
        print(f"Total Row Groups to process: {num_row_groups}")

        # 2. Iterate through Row Groups (just like the C++ code)
        for row_group_id in range(num_row_groups):
            
            # Read ONLY the target column for this specific row group
            table = parquet_file.read_row_group(row_group_id, columns=[target_col_name])
            column_data = table.column(0)
            
            # 3. Stream through the values
            for val in column_data:
                # val.is_valid is Python's equivalent of checking the definition level
                # It safely skips Null/Empty values without us needing to do math
                if val.is_valid:
                    native_val = val.as_py()
                    
                    # Match the C++ string formatting behavior
                    if isinstance(native_val, bool):
                        str_val = "true" if native_val else "false"
                    elif isinstance(native_val, bytes):
                        str_val = native_val.decode('utf-8')
                    else:
                        str_val = str(native_val)
                        
                    # Append directly as a tuple for psycopg2
                    records.append((str_val, file_path, row_group_id))

    except Exception as e:
        print(f"Parquet Extraction Error: {e}")
        
    return records


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