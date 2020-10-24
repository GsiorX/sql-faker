import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='python-sql-faker',
    version='0.2',
    author="Jakub Gąsiorek",
    author_email="kubagas@gmail.com",
    description="A tool to generate Databases with fake data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/GsiorX/sql-faker",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
