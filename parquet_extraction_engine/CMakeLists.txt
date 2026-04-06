cmake_minimum_required(VERSION 3.10)
project(ParquetExtractor)

# Force C++17 standard required by Apache Arrow
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the Arrow and Parquet packages we installed earlier
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# Define the executable
add_executable(extractor main.cpp)

# Link the libraries to our executable
target_link_libraries(extractor PRIVATE Arrow::arrow_shared Parquet::parquet_shared)