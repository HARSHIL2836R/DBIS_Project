#include <iostream>
#include <string>
#include <memory>
#include <fstream>
// Apache Arrow & Parquet Headers
#include <arrow/io/file.h>
#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include <parquet/exception.h>

void extract_index_coordinates(const std::string& file_path, int target_col_idx) {
    try {
        // File I/O: Open the target file
        std::ofstream outfile("extracted_coordinates.txt");
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file_path));

        // Open the Parquet reader to parse the footer metadata
        std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::Open(infile);
        std::shared_ptr<parquet::FileMetaData> file_metadata = reader->metadata();

        int num_row_groups = file_metadata->num_row_groups();
        std::cout << "Successfully opened: " << file_path << "\n";
        std::cout << "Total Row Groups to process: " << num_row_groups << "\n\n";

        // Isolate coordinates (Row Group ID and Values)
        for (int row_group_id = 0; row_group_id < num_row_groups; row_group_id++) {
            
            std::shared_ptr<parquet::RowGroupReader> rg_reader = reader->RowGroup(row_group_id);
            std::shared_ptr<parquet::ColumnReader> col_reader = rg_reader->Column(target_col_idx);

            // In Parquet, strings are stored as ByteArrays
            parquet::ByteArrayReader* ba_reader = static_cast<parquet::ByteArrayReader*>(col_reader.get());

            int16_t def_level, rep_level;
            parquet::ByteArray value;
            int64_t values_read = 0;

            // Stream through the values
            while (ba_reader->HasNext()) {
                ba_reader->ReadBatch(1, &def_level, &rep_level, &value, &values_read);
                
                if (values_read > 0) {
                    std::string indexed_value(reinterpret_cast<const char*>(value.ptr), value.len);
                    
                    outfile << "{ value: " << indexed_value 
                            << ", file_path: " << file_path 
                            << ", row_group_id: " << row_group_id << " }\n";
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Parquet Extraction Error: " << e.what() << std::endl;
    }
}


int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: ./extractor <path_to_parquet_file> <target_column_index>\n";
        return 1;
    }

    // Read the file path from the first argument
    std::string test_file = argv[1];
    
    // Read the target column index from the second argument
    int target_column = std::stoi(argv[2]); 

    extract_index_coordinates(test_file, target_column);

    return 0;
}