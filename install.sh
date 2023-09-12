#!/bin/bash

# This script will eventually be replaced with `databricks labs install ucx` command.

# Initialize an empty array to store Python 3 binary paths
python3_binaries=()

# Split the $PATH variable into an array using ':' as the delimiter
IFS=':' read -ra path_dirs <<< "$PATH"

# Iterate over each directory in the $PATH
for dir in "${path_dirs[@]}"; do
    # Construct the full path to the python3 binary in the current directory
    python3_path="${dir}/python3"

    # Check if the python3 binary exists and is executable
    if [ -x "$python3_path" ]; then
        python3_binaries+=("$python3_path")
    fi
done

if [ -z "${python3_binaries[*]}" ]; then
    echo "[!] No Python binaries detected"
    exit 1
fi

# Check versions for all Python binaries found
python_versions=()
for python_binary in "${python3_binaries[@]}"; do
    python_version=$("$python_binary" --version | awk '{print $2}')
    python_versions+=("$python_version -> $(realpath "$python_binary")")
done

IFS=$'\n' python_versions=($(printf "%s\n" "${python_versions[@]}" | sort -V))

py="/dev/null"
for version_and_binary in "${python_versions[@]}"; do
    echo "[i] found Python $version_and_binary"
    IFS=" -> " read -ra parts <<< "$version_and_binary"
    py="${parts[2]}"
done

echo "[i] latest python is $py"

tmp_dir=$(mktemp -d)

# Create isolated Virtualenv with the latest Python version
# in the ephemeral temporary directory
$py -m venv "$tmp_dir"

. "$tmp_dir/bin/activate"

# Use the Python from Virtualenv
py="$tmp_dir/bin/python"

echo "[+] installing dependencies within ephemeral Virtualenv: $tmp_dir"
# Install all project dependencies, so that installer can proceed
$py -m pip install --quiet -e .

# Invoke python module of the install app directly,
# without console_scripts entrypoint
$py -m databricks.labs.ucx.install

rm -r "$tmp_dir"
