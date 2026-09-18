from setuptools import find_packages, setup

setup(
    name="full-mc-production",
    version="1.0.0",
    description="CMS production planning and CERN/IHEP batch execution",
    python_requires=">=3.9",
    package_dir={"": "src"},
    packages=find_packages("src"),
    package_data={"full_mc.resources": ["*.json", "profiles/*.json"]},
    entry_points={"console_scripts": ["mcprod=full_mc.cli:main"]},
)
