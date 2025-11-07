from cx_Freeze import setup, Executable

include_files = [
    "config.ini",
    ("log_manager/logging_configs", "logging_configs"),
]

executables = [Executable("main.py", target_name="Notifly.exe")]


setup(
    name="Notifly",
    version="1.0",
    description="Internal Notifaction Application",
    options={
        "build_exe": {
            "build_exe": r"C:\Users\abdalad\Downloads\Notifly\run",
            "packages": [ 
                "log_manager", 
                "pyodbc",
                "sqlalchemy",
                "pandas",
                "zipfile",
                "msilib",
                "urllib3",
                "requests"
            ],
            "excludes": ["tkinter", "test", "unittest", "pytest"],
            "include_files": include_files,
            "optimize": 2,
        }
    },
    executables=executables,
)
