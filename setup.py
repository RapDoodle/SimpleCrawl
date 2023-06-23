from setuptools import setup, find_packages

setup(
    version="1.0",
    name="SimpleCrawl",
    packages=find_packages(),
    py_modules=["simple_crawl"],
    author="Bohui WU",
    install_requires=[
        'tqdm',
        'pymongo',
        'beautifulsoup4'
    ],
    description="Crawl the web.",
    entry_points={
        'console_scripts': ['scrawl=simple_crawl.scrawl:_main'],
    },
    include_package_data=True,
)
