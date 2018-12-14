import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="slurm",
    version="0.0.1",
    author="Mario Belledonne",
    author_email="mbelledonne@gmail.com",
    description="Job submission wrapper for SLURM.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages = ['slurmpy'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
