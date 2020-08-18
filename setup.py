import setuptools

setuptools.setup(
    name='mothrpy',
    version='0.1.0',
    author='James Arnold',
    author_email='james@rs21.io',
    description='Client library for interacting with MOTHR',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'gql==3.0.0a1',
        'redis'
    ],
    extras_require={
        'dev': [
            'mock',
            'pytest',
            'pytest-cov',
            'pytest-mypy'
        ],
        'listener': [
            'gevent'
        ]
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent'
    ],
    python_requires='>=3.6'
)
