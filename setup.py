from setuptools import setup, find_packages
import os

version = '1.6.0-rc1'

setup(
    name='org.bccvl.tasks',
    version=version,
    description="BCCVL Tasks",
    # long_description=open("README.txt").read() + "\n" +
    #                  open(os.path.join("docs", "HISTORY.txt")).read(),
    # Get more strings from
    # http://pypi.python.org/pypi?:action=list_classifiers
    classifiers=[
        "Programming Language :: Python",
    ],
    keywords='',
    author='',
    author_email='',
    #url='http://svn.plone.org/svn/collective/',
    license='GPL',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    namespace_packages=['org', 'org.bccvl'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'setuptools',  # distribute
        'celery',
        'backports.ssl_match_hostname',
    ],
    extras_require={
        'bccvl': [
            'org.bccvl.site'
        ]
    }
)
