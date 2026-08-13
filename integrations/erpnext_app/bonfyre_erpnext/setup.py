from setuptools import find_packages, setup

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

from bonfyre_erpnext import __version__

setup(
    name="bonfyre_erpnext",
    version=__version__,
    description="Bridges ERPNext documents into the Bonfyre native estate",
    author="Bonfyre",
    author_email="engineering@bonfyre.internal",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=install_requires,
)
