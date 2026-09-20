"""Entry point handed to spark-submit (see streaming/Dockerfile → CMD).

spark-submit starts the JVM, then runs this file with the Spark image's Python.
Its folder is on sys.path, so the `analyzer` package next to it imports as usual.
Everything else is in analyzer/main.py.
"""

from analyzer.main import main

if __name__ == "__main__":
    main()
