from setuptools import setup

setup(
    name="avroc",
    packages=[
        "avroc",
        "avroc.codegen",
        "avroc.runtime",
    ],
    install_requires=["fastavro"],
    tests_require=["pytest", "fastavro"],
)
