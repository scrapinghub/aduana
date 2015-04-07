Copy pack-bcolz.py to the directory with the data and run it inside there.
It will generate several directories of type:
   part-r-00XXX-bcolz

Inside this directory:

1. Compile the cython extension
   python setup.py build_ext --inplace

2. Run the script:
   python main.py path_to_the_data/*-bcolz

