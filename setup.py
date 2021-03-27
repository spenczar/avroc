from setuptools import setup

docs_require = ['sphinx']
publish_require = ['twine']

setup(
    name="avroc",
    version="0.1.0",
    packages=[
        "avroc",
        "avroc.bin",
        "avroc.codegen",
        "avroc.runtime",
    ],
    install_requires=["python-snappy", "zstandard"],
    tests_require=["pytest", "fastavro"],
    extras_require={
        'doc': docs_require,
        'publish': publish_require,
    },
    entry_points={
        "console_scripts": [
            "avroc-cli=avroc.bin.avroc_cli:main",
        ]
    },
)
