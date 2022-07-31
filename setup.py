import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="bigquery-glue",
    version="0.0.1",

    description="BigQuery Glue Job",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="bmaguiraz",

    package_dir={"": "lib/glue/packages"},
    packages=setuptools.find_packages(where="packages"),

    install_requires=[
        "aws-cdk-lib>=1.140.0",
        "constructs>=10.0.0"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
