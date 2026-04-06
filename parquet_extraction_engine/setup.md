# Setup Instructions

## Installing Arrow and Parquet Libraries

### macOS (using Homebrew)

1. **Install Homebrew** if you do not already have it:
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. **Install Apache Arrow**:
   ```bash
   brew update
   brew install apache-arrow
   ```

   Homebrew installs the Arrow libraries and Parquet support required by this project.

3. **Verify the installation**:
   ```bash
   brew list | grep arrow
   ```

---

### Linux (Ubuntu/Debian)

1. **Install Apache Arrow and Parquet**:
   ```bash
   sudo apt-get install libarrow-dev libparquet-dev
   ```

   Or use the Apache Arrow package source:

   ```bash
   wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb
   sudo apt-get install -y ./apache-arrow-apt-source-latest-noble.deb
   sudo apt-get update
   sudo apt-get install -y libarrow-dev libparquet-dev
   ```

2. **If you still get dependency issues**:
   ```bash
   sudo apt-get install -y aptitude
   sudo aptitude install -y libarrow-dev libparquet-dev
   ```

3. **Verify installation**:
   ```bash
   dpkg -l | grep arrow
   dpkg -l | grep parquet
   ```

---

## Build and Run

1. **Create a build directory**:
   ```bash
   mkdir build
   cd build
   ```

2. **Configure and compile the project**:
   ```bash
   cmake ..
   make
   ```

3. **Run the executable with appropriate input parameters**:
   ```bash
   ./extractor <target_parquet_file_path> <target_column_index>
   ```

4. **Cleaning the working directory**:
    ```bash
    rm -rf build
    rm Makefile
    ```

---

## Finding Column Indices

- Use the `read_parquet.py` script in the `data/` directory.
- Update the file path inside the script to point to your parquet file.
- Run:
  ```bash
  python3 read_parquet.py
  ```
- The script will print the column names and their corresponding indices.
