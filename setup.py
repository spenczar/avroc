from setuptools import setup

docs_require = ['sphinx']

setup(
    name="avroc",
    packages=[
        "avroc",
        "avroc.codegen",
        "avroc.runtime",
    ],
    install_requires=["python-snappy", "zstandard"],
    tests_require=["pytest", "fastavro"],
    extras_require={
        'doc': docs_require,
    },
)
