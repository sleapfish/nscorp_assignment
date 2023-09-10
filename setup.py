from setuptools import setup

setup(name='nscorp_demo',
      version='0.0.1',
      description='Sample Pyspark App for nscorp demo',
      url='',
      author='Samir Jugo',
      author_email='samir.jugo5@gmail.com',
      packages=['pipelines', 'pipelines.transformations', 'pipelines.jobs'],
      zip_safe=False)
