import os


from app.multfilesapp import SimpleApp01

os.environ['PYSPARK_PYTHON'] = "./app.pex"


if __name__ == '__main__':
  SimpleApp01(name="SimpleApp01").start()