from flask import Flask, request, send_file, jsonify
import os
import subprocess

app = Flask(__name__)
HDFS_BASE_DIR = ""  # Change this to your HDFS base directory

def hdfs_exists(path):
    cmd = ["hdfs", "dfs", "-test", "-e", path]
    return subprocess.run(cmd).returncode == 0

def is_hdfs_dir(path):
    cmd = ["hdfs", "dfs", "-test", "-d", path]
    return subprocess.run(cmd).returncode == 0

@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files.get("file")
    path = request.form.get("path")
    if not file or not path:
        return jsonify({"error": "Missing file or path parameter"}), 400
    
    hdfs_path = f"{HDFS_BASE_DIR}/{path}/{file.filename}"
    local_path = f"/tmp/{file.filename}"
    file.save(local_path)
    
    cmd = ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path]
    subprocess.run(cmd, check=True)
    os.remove(local_path)
    
    if not hdfs_exists(hdfs_path):
        return jsonify({"error": "File upload failed"}), 500
    
    return jsonify({"message": "File uploaded to HDFS successfully", "path": hdfs_path})

@app.route("/mkdir", methods=["POST"])
def create_directory():
    data = request.get_json()
    path = data.get("path")
    if not path:
        return jsonify({"error": "Missing 'path' parameter"}), 400
    
    hdfs_path = f"{HDFS_BASE_DIR}/{path}"
    cmd = ["hdfs", "dfs", "-mkdir", "-p", hdfs_path]
    subprocess.run(cmd, check=True)
    
    if not is_hdfs_dir(hdfs_path):
        return jsonify({"error": "Directory creation failed"}), 500
    
    return jsonify({"message": "Directory created in HDFS", "path": hdfs_path})

@app.route("/list", methods=["POST"])
def list_contents():
    data = request.get_json()
    path = data.get("path", "")
    hdfs_path = f"{HDFS_BASE_DIR}/{path}".rstrip("/")

    print(f"Listing contents of: {hdfs_path}")
    if not hdfs_exists(hdfs_path):
        return jsonify({"error": "Path does not exist"}), 400

    cmd = ["hdfs", "dfs", "-ls", hdfs_path]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        return jsonify({"error": result.stderr.strip()}), 500

    lines = result.stdout.strip().split("\n")[1:]  # skip first line (like "Found X items")

    entries = []
    for line in lines:
        parts = line.split(maxsplit=7)
        if len(parts) < 8:
            continue  # skip malformed lines

        permission, _, owner, group, size, date, time, full_path = parts
        entry_type = "DIRECTORY" if permission.startswith("d") else "FILE"
        name = full_path.split("/")[-1]

        entries.append({
            "name": name,
            "path": full_path,
            "type": entry_type,
            "permission": permission,
            "owner": owner,
            "group": group,
            "size": size,
            "lastModified": f"{date} {time}",
            "replication": "-",      # add actual value if needed
            "blockSize": "-",        # add actual value if needed
            "fileDescription": "N/A" # replace if you store descriptions elsewhere
        })

    print(f"Entries found: {entries}")
    return jsonify({ "contents": entries })

@app.route("/delete", methods=["POST"])
def delete_path():
    data = request.get_json()
    path = data.get("path")
    if not path:
        return jsonify({"error": "Missing 'path' parameter"}), 400
    
    hdfs_path = f"{HDFS_BASE_DIR}/{path}"
    if not hdfs_exists(hdfs_path):
        return jsonify({"error": "Path does not exist"}), 400
    
    cmd = ["hdfs", "dfs", "-rm", "-r", hdfs_path]
    subprocess.run(cmd, check=True)
    
    if hdfs_exists(hdfs_path):
        return jsonify({"error": "Deletion failed"}), 500
    
    return jsonify({"message": "Deleted from HDFS", "path": hdfs_path})

@app.route("/rename", methods=["POST"])
def rename_path():
    data = request.get_json()
    old_path = data.get("old_path")
    new_path = data.get("new_path")
    
    if not old_path or not new_path:
        return jsonify({"error": "Missing 'old_path' or 'new_path' parameter"}), 400
    
    old_hdfs_path = f"{HDFS_BASE_DIR}/{old_path}"
    new_hdfs_path = f"{HDFS_BASE_DIR}/{new_path}"
    if not hdfs_exists(old_hdfs_path):
        return jsonify({"error": "Source path does not exist"}), 400
    
    cmd = ["hdfs", "dfs", "-mv", old_hdfs_path, new_hdfs_path]
    subprocess.run(cmd, check=True)
    
    if not hdfs_exists(new_hdfs_path):
        return jsonify({"error": "Rename failed"}), 500
    
    return jsonify({"message": "Renamed in HDFS", "old_path": old_path, "new_path": new_path})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
