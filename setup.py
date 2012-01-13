"""Setup
"""
import os
from setuptools import setup, find_packages

def read(*rnames):
    text = open(os.path.join(os.path.dirname(__file__), *rnames)).read()
    return unicode(text, 'utf-8').encode('ascii', 'xmlcharrefreplace')

setup (
    name='mongopersist',
    version='0.5.2',
    author = "Stephan Richter",
    author_email = "stephan.richter@gmail.com",
    description = "Mongo Persistence Backend",
    long_description=read('src', 'mongopersist', 'README.txt'),
    license = "ZPL 2.1",
    keywords = "mongo persistent ",
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Framework :: ZODB',
        'License :: OSI Approved :: Zope Public License',
        'Natural Language :: English',
        'Operating System :: OS Independent'],
    packages = find_packages('src'),
    package_dir = {'':'src'},
    extras_require = dict(
        test = (
            'zope.app.testing',
            'zope.testing',
            ),
        zope = (
            'rwproperty',
            'zope.container',
            ),
        ),
    install_requires = [
        'ZODB3',
        'lru',
        'pymongo',
        'setuptools',
        'zope.dottedname',
        'zope.interface',
    ],
    include_package_data = True,
    zip_safe = False,
    )
