from setuptools import setup, find_packages

setup(
    name="cocoa_code",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "apache-beam[gcp]==2.61.0",
        "setuptools>=75.3.0",
        "annotated-types==0.7.0",
    ],
)
