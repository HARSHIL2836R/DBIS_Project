### Installing arrow and parquet libraries
To install the Apache Arrow and Parquet libraries, you can follow these steps:

1. **Install Apache Arrow and Parquet**:
   - You can install Apache Arrow using package managers or by building from source. For example, on Ubuntu, you can use:
     ```bash
     sudo apt-get install libarrow-dev libparquet-dev
     ```
   - Alternatively, 
        ```bash
        wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb
        sudo apt-get install -y ./apache-arrow-apt-source-latest-noble.deb
        sudo apt-get update
        sudo apt-get install -y libarrow-dev libparquet-dev
        ```
        
2. **If still there is some error**:
    -- sudo apt-get install -y aptitude
    -- sudo aptitude install -y libarrow-dev libparquet-dev   (this will show you to downgrade some packages, just select yes (the second time it asks))

3. **Verify Installation**:
   - After installation, you can verify that the libraries are installed correctly by checking their versions:
     ```bash
     dpkg -l | grep arrow
     dpkg -l | grep parquet
     ```

4. **How to run**
    - make a directory for building the project using cmake
        ```bash
        mkdir build
        cd build
        cmake ..
        make 
        ```
    - This will make the executable file (extractor) in the build directory. You can run it using:
        ```bash
        ./extractor
        ```

### To change the indexing column 
    - By default the target column (line 64 in main.cpp) is set to 0 (order_id). You can change it to any other column index as per your requirement. Just make sure to recompile the code after making changes.
    - To get the index you can use the read_parquet.py file in the data directory to read the parquet file and check the column names and their corresponding indices. (change the file path in read_parquet.py to point to your parquet file and run it to see the column names and indices)