import os
import toml
from setuptools import find_packages, setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


# copy from https://github.com/EthTx/ethtx/blob/master/setup.py
def load_requirements(fname):
    """Load requirements from file."""
    try:
        with open(fname, "r") as fh:
            pipfile = fh.read()
        pipfile_toml = toml.loads(pipfile)
    except FileNotFoundError:
        return []

    try:
        required_packages = pipfile_toml["packages"].items()
    except KeyError:
        return []

    packages = []
    for pkg, ver in required_packages:
        package = pkg
        if isinstance(ver, str) and ver != "*":
            package += ver
        elif isinstance(ver, dict) and len(ver) == 1:
            k, v = list(ver.items())[0]
            package += f" @ {k}+{v}#egg={pkg}"
        packages.append(package)
    return packages


long_description = read("README.md") if os.path.isfile("README.md") else ""

setup(
    name="nop",
    version="0.1.6",
    author="Wenbiao Zheng",
    author_email="delweng@gmail.com",
    description="A NFT Orderbook Parser",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jsvisa/nop",
    packages=find_packages(
        exclude=[
            "bin",
            "logs",
            "supervisord",
            "testdata",
            "tests",
        ]
    ),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6,<4",
    install_requires=load_requirements("Pipfile"),
)
