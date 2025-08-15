from cx_Freeze import setup, Executable

# Replace with your script name and other configurations
executables = [Executable("main.py")]

setup(
    name="MedicareRateMismatchUploader",
    version="1.0",
    description="Fetches and uploads Medicare rate mismatch data to OneDrive",
    executables=executables,
)
