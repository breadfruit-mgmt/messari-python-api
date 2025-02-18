from setuptools import setup

# read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


setup(
    name='messariV2',
    version='0.0.1',
    packages=['messariV2',
              'messariV2.messari',
              'messariV2.defillama'],
    url='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    package_data={'messariV2': ['mappings/messari_to_dl.json']},
    license='MIT`',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    author='Roberto Talamas, Michael Kremer',
    author_email='roberto.talamas@gmail.com, kremeremichael@gmail.com',
    description='Messari API'
)
