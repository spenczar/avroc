from setuptools import setup

docs_require = ['sphinx']
publish_require = ['twine']

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="avroc",
    version="0.2.0",
    description="A library for fast Avro serialization and deserialization",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Spencer Nelson",
    author_email="s@spencerwnelson.com",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    project_urls={
        "Bug Tracker": "https://github.com/spenczar/avroc/issues",
        "Documentation": "https://avroc.readthedocs.io/",
        "Repository": "https://github.com/spenczar/avroc",
    },
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
