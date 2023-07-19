import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def install_uc_migration_toolkit(parent_folder_path: Path = Path("..")):
    # Save the original working directory
    original_cwd = Path.cwd()
    ipython = get_ipython()  # noqa: F821
    from databricks.sdk.runtime import dbutils  # noqa: F821, E402

    ipython.run_cell_magic("pip", f"install hatch")

    # Create a temporary directory with the prefix 'hatch-build'
    with tempfile.TemporaryDirectory(prefix='hatch-build') as temp_dir:
        temp_path = Path(temp_dir)

        # Copy the content of the parent folder into the temporary directory
        parent_folder_path = Path(parent_folder_path)
        for item in parent_folder_path.iterdir():
            if os.access(item, os.R_OK):
                if item.is_file():
                    shutil.copy2(item, temp_path / item.name)
                elif item.is_dir():
                    shutil.copytree(item, temp_path / item.name)

        # Change the working directory to the temporary directory
        os.chdir(temp_dir)

        # Run the 'hatch' command with the specified arguments
        try:
            result = subprocess.run(["hatch", "-c", "-t", "wheel"], capture_output=True, text=True)

            if result.returncode != 0:
                # Raise an exception with the error message from the shell
                raise Exception(result.stderr.strip())

            wheel_file = list(temp_path.glob("dist/*.whl"))[0]
            ipython.run_cell_magic("pip", f"install {str(wheel_file.absolute())}")
            dbutils.library.restartPython()

        except Exception as e:
            # Return to the original working directory before raising the exception
            os.chdir(original_cwd)
            raise e

    # Return to the original working directory
    os.chdir(original_cwd)
