from flask import Flask, request, send_file, jsonify
import os
import subprocess

app = Flask(__name__)
HDFS_BASE_DIR = "/user/avinash"  # Change this to your HDFS base directory

@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    
    local_path = f"/tmp/{file.filename}"
    hdfs_path = f"{HDFS_BASE_DIR}/{file.filename}"
    file.save(local_path)
    
    cmd = ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path]
    subprocess.run(cmd, check=True)
    os.remove(local_path)
    
    return jsonify({"message": "File uploaded to HDFS successfully", "path": hdfs_path})

@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    hdfs_path = f"{HDFS_BASE_DIR}/{filename}"
    local_path = f"/tmp/{filename}"
    
    cmd = ["hdfs", "dfs", "-get", hdfs_path, local_path]
    subprocess.run(cmd, check=True)
    
    return send_file(local_path, as_attachment=True)

@app.route("/mkdir", methods=["POST"])
def create_directory():
    data = request.get_json()
    dirname = data.get("dirname")
    if not dirname:
        return jsonify({"error": "Missing 'dirname' parameter"}), 400
    
    hdfs_path = f"{HDFS_BASE_DIR}/{dirname}"
    cmd = ["hdfs", "dfs", "-mkdir", "-p", hdfs_path]
    subprocess.run(cmd, check=True)
    
    return jsonify({"message": "Directory created in HDFS", "path": hdfs_path})

@app.route("/list", methods=["GET"])
def list_files():
    cmd = ["hdfs", "dfs", "-ls", HDFS_BASE_DIR]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    files = [line.split()[-1] for line in result.stdout.strip().split("\n")[1:]]
    
    return jsonify({"files": files})

@app.route("/delete", methods=["POST"])
def delete_file():
    data = request.get_json()
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "Missing 'filename' parameter"}), 400
    
    hdfs_path = f"{HDFS_BASE_DIR}/{filename}"
    cmd = ["hdfs", "dfs", "-rm", "-r", hdfs_path]
    subprocess.run(cmd, check=True)
    
    return jsonify({"message": "Deleted from HDFS", "path": hdfs_path})

@app.route("/rename", methods=["POST"])
def rename_file():
    data = request.get_json()
    old_name = data.get("old_name")
    new_name = data.get("new_name")
    
    if not old_name or not new_name:
        return jsonify({"error": "Missing 'old_name' or 'new_name' parameter"}), 400
    
    old_path = f"{HDFS_BASE_DIR}/{old_name}"
    new_path = f"{HDFS_BASE_DIR}/{new_name}"
    
    cmd = ["hdfs", "dfs", "-mv", old_path, new_path]
    subprocess.run(cmd, check=True)
    
    return jsonify({"message": "Renamed in HDFS", "old_name": old_name, "new_name": new_name})

@app.route("/copy", methods=["POST"])
def copy_file():
    data = request.get_json()
    src = data.get("src")
    dest = data.get("dest")
    
    if not src or not dest:
        return jsonify({"error": "Missing 'src' or 'dest' parameter"}), 400
    
    src_path = f"{HDFS_BASE_DIR}/{src}"
    dest_path = f"{HDFS_BASE_DIR}/{dest}"
    
    cmd = ["hdfs", "dfs", "-cp", src_path, dest_path]
    subprocess.run(cmd, check=True)
    
    return jsonify({"message": "Copied in HDFS", "src": src, "dest": dest})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
