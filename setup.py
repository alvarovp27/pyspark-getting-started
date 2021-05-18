import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyspark_getting_started",
    version="0.0.1",
    author="Alvaro Valencia-Parra",
    description="Getting started on Apache Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alvarovp27/knowledge-basek",
    classifiers=[],
    keywords=["pyspark", "getting-started", "apache spark"],
    python_requires='>=3.7',
    packages=setuptools.find_packages()
)