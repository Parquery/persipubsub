"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""
import os

from setuptools import find_packages, setup

import persipubsub_meta

here = os.path.abspath(os.path.dirname(__file__))  # pylint: disable=invalid-name

with open(os.path.join(here, 'README.rst'), encoding='utf-8') as fid:
    long_description = fid.read().strip()  # pylint: disable=invalid-name

setup(
    name=persipubsub_meta.__title__,
    version=persipubsub_meta.__version__,
    description=persipubsub_meta.__description__,
    long_description=long_description,
    url=persipubsub_meta.__url__,
    author=persipubsub_meta.__author__,
    author_email=persipubsub_meta.__author_email__,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    license='License :: OSI Approved :: MIT License',
    keywords=
    'persistent publisher subscriber lmdb MQ message queue thread-safe process-safe',
    packages=find_packages(
        exclude=['tests', 'obsolete', 'messages', 'example']),
    install_requires=[
        # yapf: disable
        'typing-extensions>=3.7.2',
        'icontract>=2.0.2,<3',
        'lmdb>=0.94,<1'
        # yapf: enable
    ],
    extras_require={
        'dev': [
            # yapf: disable
            'mypy==0.670',
            'pylint==2.2.2',
            'yapf==0.26.0',
            'tox>=3.7.0',
            'coverage>=4.5.2,<5',
            'pydocstyle>=3.0.0,<4',
            'pyicontract-lint>=2.0.0,<3',
            'docutils>=0.14,<1',
            'isort>=4.3.4,<5',
            'pygments>=2.3.1,<3',
            'twine>=1.12.1,<2',
            'setuptools>=40.8.0,<41',
            'wheel>=0.33.0,<1',
            'temppathlib>=1.0.3,<2'
            # yapf: enable
        ]
    },
    py_modules=['persipubsub', 'persipubsub_meta'],
    package_data={
        "persipubsub": ["typed"],
        '.': ['LICENSE', 'README.rst']
    })
