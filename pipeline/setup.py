"""
Setup.py file.
Install once-off with:  "pip install ."
For development:        "pip install -e .[dev]"
"""
import setuptools


with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

PROJECT_NAME = "pipeline"

setuptools.setup(
    name=PROJECT_NAME,
    version="0.1",
    author="Jacopo Margutti",
    author_email="jmargutti@redcross.nl",
    description="file-share-to-blob",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=install_requires,
    extras_require={
        "dev": [
            "black",
            "flake8"
        ],
    },
    entry_points={
        'console_scripts': [
            f"file-share-to-blob = {PROJECT_NAME}.pipeline:main",
        ]
    }
)