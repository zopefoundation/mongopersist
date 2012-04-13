"""Setup
"""
import os
from setuptools import setup, find_packages

def read(*rnames):
    text = open(os.path.join(os.path.dirname(__file__), *rnames)).read()
    return unicode(text, 'utf-8').encode('ascii', 'xmlcharrefreplace')

setup (
    name='mongopersist',
    version='0.7.1',
    author = "Stephan Richter",
    author_email = "stephan.richter@gmail.com",
    description = "Mongo Persistence Backend",
    long_description=(
        read('src', 'mongopersist', 'README.txt')
        + '\n\n' +
        read('CHANGES.txt')
        ),
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
            'ZODB3',
            ),
        zope = (
            'rwproperty',
            'zope.container',
            ),
        ),
    install_requires = [
        'transaction >=1.1.0',
        'repoze.lru',
        'pymongo',
        'setuptools',
        'zope.dottedname',
        'zope.interface',
        'zope.exceptions >=3.7.1', # required for extract_stack
    ],
    include_package_data = True,
    zip_safe = False,
    entry_points = '''
    [console_scripts]
    profile = mongopersist.performance:main
    ''',
    )
