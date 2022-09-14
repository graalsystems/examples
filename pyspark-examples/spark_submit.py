"""
spark_submit.py
"""
import logging

import src.pyspark_examples.processing

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    logging.debug('Run Spark job!')
    src.pyspark_examples.processing.run()
