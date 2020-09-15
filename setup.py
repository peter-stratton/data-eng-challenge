from setuptools import find_packages, setup

setup(
    name='nhldata',
    version='0.0.1',
    description='A command line utility to collect NHL game data',
    packages=find_packages(),
    install_requires=[
        'requests>=2.24.0,<=2.25.0',
        'pandas>=1.1.0,<=1.2.0',
        'boto3>=1.14.38,<=1.15.0',
        'click>=7.1.2,<=7.2.0'
    ],
    entry_points={
        'console_scripts': [
            'nhldata = nhldata.app:main'
        ]
    },
    python_requires='>=3.8'
)
