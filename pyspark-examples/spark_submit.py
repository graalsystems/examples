"""
spark_submit.py
"""
import logging

from tools.processing import run

logging.basicConfig(level=logging.DEBUG)


def main():
    """Main script definition.
    :return: None
    """
    logging.debug('Run Spark job!')
    run()


# entry point for PySpark
if __name__ == "__main__":
    main()
