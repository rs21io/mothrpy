import setuptools

with open("README.md") as f:
    long_description = f.read()

setuptools.setup(
    name="mothrpy",
    version="0.2.0",
    author="James Arnold",
    author_email="james@rs21.io",
    description="Client library for interacting with MOTHR",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=["gql[requests,websockets]==3.0.0a4"],
    extras_require={
        "dev": ["mock", "pytest", "pytest-cov", "pytest-mypy", "pytest-pylint"]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
