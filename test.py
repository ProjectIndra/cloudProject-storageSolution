import pyarrow.fs as fs

hdfs_host = "0.0.0.0"  # Use actual HDFS NameNode IP
hdfs_uri = f"hdfs://{hdfs_host}"  # No explicit port
hdfs_port = 9000

# Connect to HDFS
hdfs = fs.HadoopFileSystem(hdfs_uri,hdfs_port)

# Test creating a directory
hdfs.create_dir("/test-pyarrow")
print("✅ Directory created successfully!")
