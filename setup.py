import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="LoadBalancer", # Replace with your own username
    version="0.4",
    author="Matias Borghi",
    author_email="borghi.matias@gmail.com",
    description="ZMQ's Load Balancer pattern implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mattborghi/load-balancer-zmq",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)