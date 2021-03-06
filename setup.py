from setuptools import setup, find_packages


tests_require = [
    'mock',
    'requests'
]

setup(
    name='org.bccvl.tasks',
    setup_requires=['setuptools_scm'],
    use_scm_version=True,
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
    # url='http://svn.plone.org/svn/collective/',
    license='GPL',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    namespace_packages=['org', 'org.bccvl'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'setuptools',
        'celery < 4',
        'org.bccvl.movelib',
    ],
    tests_require=tests_require,
    extras_require={
        'bccvl': [
            'org.bccvl.site'
        ],
        'exports': [
            'requests',
            'requests-oauthlib',
            'google-api-python-client',
            'dropbox',
        ],
        'metadata': [
            'GDAL',
            'python-xmp-toolkit',
        ],
        'http': [
            'org.bccvl.movelib[http]',
        ],
        'scp': [
            'org.bccvl.movelib[scp]',
        ],
        'swift': [
            'org.bccvl.movelib[swift]',
        ],
        'test': tests_require
    }
)
