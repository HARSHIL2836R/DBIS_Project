#include <iostream>
#include <string>
#include <memory>
#include <fstream>
// Apache Arrow & Parquet Headers
#include <arrow/io/file.h>
#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include <parquet/exception.h>

// --- 1. HELPER FUNCTIONS FOR FORMATTING VALUES ---
std::string format_value(int32_t v) { return std::to_string(v); }
std::string format_value(int64_t v) { return std::to_string(v); }
std::string format_value(float v) { return std::to_string(v); }
std::string format_value(double v) { return std::to_string(v); }
std::string format_value(bool v) { return v ? "true" : "false"; } // Added Boolean formatter
std::string format_value(const parquet::ByteArray& v) {
    return std::string(reinterpret_cast<const char*>(v.ptr), v.len);
}

// --- 2. TEMPLATE FOR PROCESSING ANY COLUMN TYPE ---
template <typename ReaderType, typename ValueType>
void process_column_chunk(parquet::ColumnReader* base_reader, std::ofstream& outfile, 
                          const std::string& file_path, int row_group_id) {
    
    ReaderType* specific_reader = static_cast<ReaderType*>(base_reader);
    
    int16_t def_level, rep_level;
    ValueType value;
    int64_t values_read = 0;

    while (specific_reader->HasNext()) {
        specific_reader->ReadBatch(1, &def_level, &rep_level, &value, &values_read);
        
        if (values_read > 0) {
            outfile << "{ value: " << format_value(value) 
                    << ", file_path: " << file_path 
                    << ", row_group_id: " << row_group_id << " }\n";
        }
    }
}

// --- 3. CORE EXTRACTION LOGIC ---
void extract_index_coordinates(const std::string& file_path, int target_col_idx) {
    try {
        std::ofstream outfile("extracted_coordinates.txt");
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file_path));

        std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::Open(infile);
        std::shared_ptr<parquet::FileMetaData> file_metadata = reader->metadata();

        int num_row_groups = file_metadata->num_row_groups();
        std::cout << "Successfully opened: " << file_path << "\n";
        std::cout << "Total Row Groups to process: " << num_row_groups << "\n\n";

        for (int row_group_id = 0; row_group_id < num_row_groups; ++row_group_id) {
            
            std::shared_ptr<parquet::RowGroupReader> rg_reader = reader->RowGroup(row_group_id);
            std::shared_ptr<parquet::ColumnReader> col_reader = rg_reader->Column(target_col_idx);

            parquet::Type::type col_type = col_reader->descr()->physical_type();

            switch (col_type) {
                case parquet::Type::BOOLEAN: // Added Boolean case
                    process_column_chunk<parquet::BoolReader, bool>(col_reader.get(), outfile, file_path, row_group_id);
                    break;
                case parquet::Type::INT32:
                    process_column_chunk<parquet::Int32Reader, int32_t>(col_reader.get(), outfile, file_path, row_group_id);
                    break;
                case parquet::Type::INT64:
                    process_column_chunk<parquet::Int64Reader, int64_t>(col_reader.get(), outfile, file_path, row_group_id);
                    break;
                case parquet::Type::FLOAT:
                    process_column_chunk<parquet::FloatReader, float>(col_reader.get(), outfile, file_path, row_group_id);
                    break;
                case parquet::Type::DOUBLE:
                    process_column_chunk<parquet::DoubleReader, double>(col_reader.get(), outfile, file_path, row_group_id);
                    break;
                case parquet::Type::BYTE_ARRAY:
                    process_column_chunk<parquet::ByteArrayReader, parquet::ByteArray>(col_reader.get(), outfile, file_path, row_group_id);
                    break;
                default:
                    std::cerr << "Warning: Unsupported physical column type encountered in Row Group " << row_group_id << ".\n";
                    break;
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

    std::string test_file = argv[1];
    int target_column = std::stoi(argv[2]); 

    extract_index_coordinates(test_file, target_column);

    return 0;
}