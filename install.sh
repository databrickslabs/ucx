#!/bin/bash

# This script will eventually be replaced with `databricks labs install ucx` command.

# Initialize an empty array to store Python binary paths
python_binaries=()

# Split the $PATH variable into an array using ':' as the delimiter
IFS=':' read -ra path_dirs <<< "$PATH"

# Iterate over each directory in the $PATH
for dir in "${path_dirs[@]}"; do
    # Construct the full path to the python binaries in the current directory
    python2_path="${dir}/python"
    python3_path="${dir}/python3"

    # Check if the python binary exists and is executable
    if [ -x "$python3_path" ]; then
        python_binaries+=("$python3_path")
    elif [ -x "$python2_path" ]; then
        python_binaries+=("$python2_path")
    fi
done

if [ -z "${python_binaries[*]}" ]; then
    echo "[!] No Python binaries detected"
    exit 1
fi

# Check versions for all Python binaries found
python_versions=()
for python_binary in "${python_binaries[@]}"; do
    python_version=$("$python_binary" --version)
    IFS=" " read -ra parts <<< "$python_version"
    if [ "${#parts[@]}" -eq "2" ]; then
      python_versions+=("${parts[1]} -> $(realpath "$python_binary")")
    else
        echo "[!] Found bad version string, skipping binary"
    fi
done

IFS=$'\n' python_versions=($(printf "%s\n" "${python_versions[@]}" | sort -V))

py="/dev/null"
for version_and_binary in "${python_versions[@]}"; do
    echo "[i] found Python $version_and_binary"
    IFS=" -> " read -ra parts <<< "$version_and_binary"
    IFS="" py="${parts[@]:2}"
done

echo "[i] latest python is $py"

tmp_dir=$(mktemp -d)

# Create isolated Virtualenv with the latest Python version
# in the ephemeral temporary directory
echo "[i] installing venv into: $tmp_dir"
$py -m venv "$tmp_dir"

# Detect which venv this is, activate and reset Python binary
if [ -f "$tmp_dir/bin/activate" ]; then
    . "$tmp_dir/bin/activate"
    py="$tmp_dir/bin/python"
elif [ -f "$tmp_dir/Scripts/activate" ]; then
    . "$tmp_dir/Scripts/activate"
    py="$tmp_dir/Scripts/python"
else
    echo "[!] Creating Python virtual environment failed"
    exit 1
fi

echo "[+] making sure we have the latest pip version"
# Always upgrade pip, so that the hatchling build backend works. Hinted by errors like
# > File "setup.py" or "setup.cfg" not found. Directory cannot be installed in editable mode
#
# See https://github.com/databrickslabs/ucx/issues/198
$py -m pip install --quiet --upgrade pip

echo "[+] installing dependencies within ephemeral Virtualenv: $tmp_dir"
# Install all project dependencies, so that installer can proceed
$py -m pip install --quiet -e .

# Invoke python module of the install app directly,
# without console_scripts entrypoint
$py -m databricks.labs.ucx.install

rm -r "$tmp_dir"