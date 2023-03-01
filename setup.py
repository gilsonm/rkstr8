import setuptools
import os


with open("README.md", "r", encoding = "utf-8") as fh:
    long_description = fh.read()

# These make there way into the wheel metadata Requires-Dist fields at wheel build-time.
# They are (conditionally) installed when the wheel is installed with pip.
requirement_path = 'requirements.txt'
rkstr8_deps = []
if os.path.isfile(requirement_path):
    with open(requirement_path) as f:
        rkstr8_deps = f.read().splitlines()

setuptools.setup(
    name = "rkstr8",
    version = "0.0.1",
    author = "Michael Gilson",
    author_email = "michael.c.gilson@gmail.com",
    description = "Workflow description language and execution platform for automating batch computing workflows",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "package URL",
    project_urls = {
        "Bug Tracker": "package issues URL",
    },
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=rkstr8_deps,
    package_dir = {"": "src"},
    packages = setuptools.find_packages(where="src"),
    python_requires = ">=3.8",
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'rkstr8 = rkstr8.__main__:cli', # cli is a function from src/rkstr8/__main__.py
        ]
    },
    cmdclass={
        'build': CustomBuildCommand
    }
)