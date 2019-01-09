# Faas Supervisor

Input / Output data manager for FaaS functions.
Works in different cloud environments and allows different data providers.

Python package available at Pypi with the name **faas-supervisor**.

## How to create a distribution package with twine
First install twine if you don't have it:

```
pip3 install twine
```

Create the distribution packages:

```
python setup.py sdist bdist_wheel
```
 
Upload the packages to Pypi:
 
```
twine upload dist/*
```