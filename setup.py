import setuptools
with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
     name='easyjobs',  
     version='PYQLVERSION', # PYQLVERSION
     packages=setuptools.find_packages(include=['easyjobs', 'easyjobs.api', 'easyjobs.brokers', 'easyjobs.producers', 'easyjobs.workers'], exclude=['build']),
     author="Joshua Jamison",
     author_email="joshjamison1@gmail.com",
     description="An easy to use, celerly-like jobs framework, for creating, distributing, and managing workloads",
     long_description=long_description,
   long_description_content_type="text/markdown",
     url="https://github.com/codemation/easyjobs",
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
     python_requires='>=3.7, <4',   
     install_requires=['easyrpc>=0.238', 'aiopyql>=0.351', 'aio-pika', 'easyschedule'], )