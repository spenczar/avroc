from setuptools import setup

setup(
    name="avroc",
    packages=[
        "avroc",
        "avroc.codegen",
        "avroc.runtime",
    ],
    install_requires=["fastavro", "python-snappy", "zstandard"],
    tests_require=["pytest", "fastavro"],
)
