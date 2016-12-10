from distutils.core import setup

classifiers = [
    'Development Status :: 3 - Alpha',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name="toyredis",
    version=__import__('toyredis').__version__,
    url='https://github.com/nakagami/toyredis/',
    classifiers=classifiers,
    keywords=['Redis'],
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    description='A Redis client',
    long_description=open('README.rst').read(),
    license="MIT",
    py_modules=['toyredis'],
)
