# DPiSAX: Massively Distributed Partitioned iSAX

## [Djamel Edine Yagoubi](http://djameledine-yagoubi.info/), [Reza Akbarinia](http://www-sop.inria.fr/members/Reza.Akbarinia/index.html), [Florent Masseglia](http://www.florent-masseglia.info/), [Themis Palpanas](http://www.mi.parisdescartes.fr/~themisp/)

[Paper](https://hal-lirmm.ccsd.cnrs.fr/lirmm-01620125/file/DPiSAX__ICDM___short_paper_.pdf)
\| [Source Code](https://github.com/djamelinfo/DPiSAX) \| [Download
.zip](https://github.com/djamelinfo/DPiSAX/zipball/master)

## Overview

Performing similarity queries on billions of time series is a challenge
requiring both efficient indexing techniques and parallelization. This
library contains a number of algorithms and optimizations to solve the
problem of indexing and querying billions of time series. This page
comes with the paper about *DPiSAX: Massively Distributed Partitioned
iSAX*. It gives links to the code and documentation of:

-   DPiSAX, DbasicPiSAX and DiSAX.
-   [iSAX2+](https://github.com/djamelinfo/iSAX2-): Java implementation
    of iSAX2+
    [\[1\]](http://www.mi.parisdescartes.fr/~themisp/isax2plus/).

You may freely use this code for research purposes, provided that you
contact us [here](mailto:djamel-edine.yagoubi@inria.fr).

## Requirements

Massively Distributed Partitioned iSAX works with [Apache
Spark](http://spark.apache.org/). In order to run DPiSAX, you must
download and install [Apache Spark
1.6.2](http://spark.apache.org/news/spark-1-6-2-released.html).

-   The Spark
    [configuration](https://github.com/djamelinfo/Tuning-Spark) used for
    our experiments.

## Building

The code has been written in Java and we used Maven to build it. Please
use the given
[pom.xml](https://github.com/djamelinfo/DPiSAX/blob/master/src/main/pom.xml)
file to build an executable JAR containing all the dependencies.

## Configuration & Usage

DPiSAX settings can be configured through the [configuration
file](https://github.com/djamelinfo/DPiSAX/blob/master/PiSAX.config).
For more information, try:

    $SPARK_HOME/bin/spark-submit --class fr.inria.sparkisax.PiSAX DP-iSAX-1.jar -h

    Usage: PDiSAX [OPTION]...

    Options:
      -f            Input file
      -g            RandomWalk Time Series Generator, you must give number of Time Series and number of elements in each Time Series.
      -G            RandomWalk Query Time Series Generator, you must give number of Time Series.
      -o            Output directory.
      -q            Comma-separated list of input query file.
      -c            Path to a file from which to load extra properties. If not specified, defaults will be used.
      --config      Change given configuration variables, NAME=VALUE (e.g fraction, partitions, normalization, wordLen, timeSeriesLength, threshold, k).
      -a            Use 1 for DPiSAX, use 3 for DbasicPiSAX.
      -A            Use 1 for (DPiSAX and DbasicPiSAX), use 2 for DiSAX.
      --ls          Parallel Linear Search.
      -p            Preprocessing. You must give input file.
      -h            Show this help message and exit.

    Full documentation at: <http://djameledine-yagoubi.info/projects/DPiSAX/>

## Datasets

We carried out our experiments on synthetic datasets using a random walk
data series generator, each data series consisting of 256 points. At
each time point the generator draws a random number from a Gaussian
distribution N(0,1), then adds the value of the last number to the new
number. You can use [Random Walk Time Series
Generator](https://github.com/djamelinfo/RandomWalk-TimeSeriesGenerator-On-Spark/wiki)
to produce a set of randomWalk datasets.

The real world data represents seismic time series collected from the
[IRIS Seismic Data Access repository](http://ds.iris.edu/data/access/).

This page maintained by [Djamel Edine
Yagoubi](http://djameledine-yagoubi.info/).
