from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name = "catnip",
    version = "0.0.678",
    author = "Panthers Insights",
    author_email = "panthersinsights@floridapanthers.com",
    description = "Facilitating repeatable internal code",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/panthersinsights-fla/catnip",
    license = "MIT",
    packages = find_packages(),
    install_requires = requirements
)