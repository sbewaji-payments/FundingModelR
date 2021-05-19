## install packages regularly used in all model development and data analysis

##set option to stop R from installing from source files
options(install.packages.check.source = "yes");

#install.packages("Rtools");
install.packages('devtools');##Install the Have you look looked at dse: Dynamic Systems Estimation? 
##In addition to the standard ARMA model it supports VAR and VARX analysis.
install.packages("rlang");
install.packages("stringi", dependencies=TRUE, INSTALL_opts = c('--no-lock'));
install.packages("stringr");
##If the above does not work download packages and install with the foloowing comands 
# install.packages("stringi_1.1.7.zip", # this will install from online
#                  configure.vars="ICUDT_DIR=C:/RPackages"); #but use the downloaded version of icudt
# install.packages("stringr_1.3.0.zip", # this will install from online
#                  configure.vars="ICUDT_DIR=C:/RPackages"); #but use the downloaded version of icudt
#install.packages("data.table");#used to store and manipiulate data in the form of a data table similar to a mySQL database table
#install.packages("ggplot2");#used to plot data
#remove.packages(c("ggplot2", "data.table"))
install.packages("plyr");# contains method and function calls to be used to filter and select subsets of the data
install.packages("lubridate");# handles that date elements of the data
install.packages("dplyr");# contains method and function calls to be used to filter and select subsets of the data
install.packages("glue");
install.packages('Rcpp');
install.packages('RJDBC');
install.packages('data.table');
install.packages('ggplot2');
install.packages("ggthemes");##themes extention to ggplot2
install.packages("ggfortify");##extention to ggplot2
install.packages("strucchange");##capture structural changes in data 
install.packages("gridExtra");#to plot a multiplot consisting of multiple  distinct plot datasets
install.packages("reshape2");#to plot a multiple series on the same ggplot more efficiently
install.packages("magrittr");#contains method/function calls to use for chaining e.g. %>%
install.packages("scales");#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
install.packages("foreach");#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
install.packages("iterators");#handles iteration through for each element of a vector/data frame/array/etc
install.packages("moments");#3rd-party library used to compute statistics such as skewness and kurtosis which are not provided in the base R package set
install.packages("smooth");
install.packages("zoo");
install.packages("xts");
install.packages("timeSeries");
install.packages("sarima"); ##SARIMA Modelling package 
install.packages("astsa"); ##alternative SARIMA Modelling package
install.packages("fGarch"); ##GARCH Modelling package 
install.packages("rugarch"); ##GARCH Modelling package 
install.packages("PerformanceAnalytics"); ##financial markets data analytics package 
#install.packages("stringr");
install.packages("foreign");#3-party package to export data into STATA/SAS/SPSS format
install.packages("xlsx");#3-party package to export data into excel xlsx format
install.packages("splusTimeDate");#installs the ‘splusTimeDate’ package that is a
#A collection of classes and methods for working with
#times and dates. The code was originally available in S-PLUS.
install.packages("qdapTools");#3-party package to carry out excel type lookups Rcmdr
install.packages("Rcmdr");#3-party package to carry out excel type lookups 
install.packages("igraph");#3-party package to carry out excel type lookups 
install.packages("networkD3");
install.packages("sqldf");
install.packages("Rtools");
install.packages("coinmarketcapr");
library('devtools');
devtools::install_github("amrrs/coinmarketcapr");
install.packages("lattice");#3-party package to generate comparitive plots as lattices 
install.packages("quantmod");#3-party package Quantitative Financial Modelling Framework for quantitiative work
install.packages("functional");#3-party package adding new columns with calculated values
install.packages("Deducer"); #alternative viewer to RStudio
install.packages("forecast"); #alternative viewer to RStudio
install.packages("latticeExtra");#3-party package to generate comparitive plots as lattices 
install.packages("tseries");#3-party package to generate comparitive plots as lattices 
install.packages("timeSeries");#3-party package to generate comparitive plots as lattices 
#install.packages("grDevices");#3-party package to fix base plots axis -- package ‘grDevices’ is not available (for R version 3.2.2)
install.packages("timeDate");#3-party package to work on date and time objects
install.packages("openxlsx");#3-party package that Simplifies the creation of Excel .xlsx files by providing a 
#high level interface to writing, styling and editing worksheets. Through
#the use of Rcpp, read/write times are comparable to the xlsx and XLConnect
#packages with the added benefit of removing the dependency on Java
install.packages("plot3D");##For visualising 3d surface and other 3d plots
install.packages("scatterplot3d", dependencies = TRUE);##For visualising 3d surface and other 3d plots
install.packages("rgl", dependencies = TRUE);##For visualising 3d surface and other 3d plots
install.packages("TSA");
install.packages("extraDistr");#3-party package containing various additional probability distributions
install.packages("creditr");#3-party package to price credit default swaps
install.packages("credule");#3-party package to price credit default swaps
install.packages("RQuantLib");#3-party package to price credit default swaps
install.packages("copula");#3-party package to price credit default swaps
install.packages("doMC");#3-party package to price credit default swaps
install.packages("neuralnet");#3-party package to do neural network based data and model analysis
install.packages("tseries");
install.packages("fImport");
install.packages("rdatamarket");
install.packages("fImport");
install.packages("pdfetch");
install.packages("Quandl");
install.packages("quantmod");
install.packages("xtable");
install.packages("gdata");
install.packages("plotly");
install.packages("nowcasting", dependencies = TRUE);

install.packages("gargle", dependencies = TRUE);

devtools::install_github("rstats-db/bigrquery");
install.packages("readr");
install.packages("vars"); #try  to  fill  a  gap  in  the  econometrics’  methods landscape of R 
#by providing the “standard” tools in the context of VAR, SVAR and SVEC analysis
install.packages("dse");
install.packages("fArma");#(Ẅurtz 2007) are made available for estimating ARIMA and VARIMA time series models
install.packages("MSBVAR");#(Brandt and Appleby 2007) provides methods for estimating frequentist 
#and Bayesian vector autoregression (BVAR) models
install.packages("janitor");##used for clean up formating and especially when the ns_to_percents function is called
                            ##Convert a numeric data.frame to row-,  column-,  or totals-wise percentages.
                            ##  
install.packages("httpuv");
install.packages("htmltools");
install.packages("bindrcpp");
install.packages("tibble");
install.packages("colourpicker");
install.packages("DT");
install.packages("rmarkdown");
install.packages("shiny", dependencies=FALSE);
install.packages("shinyjs");
install.packages("threejs");
install.packages("evaluate");
install.packages("StanHeaders");
install.packages("RcppEigen");
install.packages("shinystan");
install.packages("bayesplot");
install.packages("knitr");
install.packages("rstan");
install.packages("bayesTFR"); ##UN package for population projections (computes fertility rates)
install.packages("bayesLife"); ##UN package for population projections (computes life expectancy rates)
install.packages("bayesPop"); ##UN package for population projections (computes population pojections)
install.packages("bayesDem"); ##UN package for population projections (The graphic user interface)
install.packages("wppExplorer"); ##UN package for population projections (The graphic user interface)
install.packages("MCMCpack");
install.packages("arm");
##install.packages("rlang");
install.packages("bayesplot");
require(bayesplot);
install.packages("shinystan");
install.packages('lmerTest')
install.packages("migest");
install.packages("backports");
install.packages("psych");
install.packages("ggpubr");
install.packages("tidyverse");
install.packages("tibble");
install.packages("Hmisc");
install.packages("corrplot");
install.packages("Mcomp");
install.packages("GA");
install.packages("ReinforcementLearning");
install.packages("reinforcelearn");
install.packages("rjags"); ##Used in tandom with JAGs to do baysian statistics
install.packages("rstan", repos = "https://cloud.r-project.org/");
install.packages("shinystan", repos = "https://cloud.r-project.org/");
install.packages("bigrquery", repos = "https://cloud.r-project.org/");


library('devtools');
install_github('fastVAR', user='jeffwong');
require(devtools)
install_version("backports", version = "1.1.0")

##For fast Bayesian analysis
library('devtools');
install_github('rmcelreath/glmer2stan');

source_url("https://github.com/stan-dev/shinystan/raw/develop/install_shinystan.R");
install_shinystan();

