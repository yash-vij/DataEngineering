{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c1fa0011-223d-43d2-aba7-2468e8a33676",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "\n",
    "logFile = \"README.txt\"  # Should be some file on your system\n",
    "spark = SparkSession.builder.appName(\"SimpleApp\").getOrCreate()\n",
    "logData = spark.read.text(logFile).cache()\n",
    "\n",
    "numAs = logData.filter(logData.value.contains('a')).count()\n",
    "numBs = logData.filter(logData.value.contains('b')).count()\n",
    "\n",
    "print(\"Lines with a: %i, lines with b: %i\" % (numAs, numBs))\n",
    "\n",
    "spark.stop()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.7"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
