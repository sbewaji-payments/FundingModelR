## install packages regularly used in all model development and data analysis

##set option to stop R from installing from source files
options(install.packages.check.source = "no");


install.packages("plyr", dependencies = TRUE);# contains method and function calls to be used to filter and select subsets of the data
install.packages("lubridate", dependencies = TRUE);# handles that date elements of the data
install.packages("dplyr", dependencies = TRUE);# contains method and function calls to be used to filter and select subsets of the data
install.packages("glue");
install.packages("stringi", dependencies=TRUE, INSTALL_opts = c('--no-lock'));
install.packages("stringr");
##If the above does not work download packages and install with the foloowing comands 
install.packages("stringi_1.1.5.zip", # this will install from online
                 configure.vars="ICUDT_DIR=C:/Users/sbewaji/Documents/RPackages") #but use the downloaded version of icudt
install.packages("stringr_1.2.0.zip", # this will install from online
                 configure.vars="ICUDT_DIR=C:/Users/sbewaji/Documents/RPackages") #but use the downloaded version of icudt
#install.packages("data.table");#used to store and manipiulate data in the form of a data table similar to a mySQL database table
#install.packages("ggplot2");#used to plot data
#remove.packages(c("ggplot2", "data.table"))
install.packages('Rcpp', dependencies = TRUE)
install.packages('ggplot2', dependencies = TRUE)
install.packages('data.table', dependencies = TRUE)
install.packages("ggthemes");##themes extention to ggplot2
install.packages("ggfortify");##extention to ggplot2
install.packages("strucchange", dependencies = TRUE);##capture structural changes in data 
install.packages("gridExtra", dependencies = TRUE)#to plot a multiplot consisting of multiple  distinct plot datasets
install.packages("reshape2", dependencies = TRUE)#to plot a multiple series on the same ggplot more efficiently
install.packages("magrittr", dependencies = TRUE);#contains method/function calls to use for chaining e.g. %>%
install.packages("scales", dependencies = TRUE);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
install.packages("foreach", dependencies = TRUE);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
install.packages("iterators", dependencies = TRUE);#handles iteration through for each element of a vector/data frame/array/etc
install.packages("moments", dependencies = TRUE);#3rd-party library used to compute statistics such as skewness and kurtosis which are not provided in the base R package set
install.packages("rlist", dependencies = TRUE);#3-party package to allow more flexible and java-collections like handling of lists in R. Allows you to save a list as a Rdata file
install.packages("scales", dependencies = TRUE);
install.packages("zoo", dependencies = TRUE);
install.packages("xts", dependencies = TRUE);
install.packages("timeSeries", dependencies = TRUE);
#install.packages("stringr", dependencies = TRUE);
install.packages("foreign", dependencies = TRUE);#3-party package to export data into STATA/SAS/SPSS format
install.packages("xlsx", dependencies = TRUE);#3-party package to export data into excel xlsx format
install.packages("qdapTools", dependencies = TRUE);#3-party package to carry out excel type lookups Rcmdr
install.packages("Rcmdr", dependencies = TRUE);#3-party package to carry out excel type lookups 
install.packages("igraph", dependencies = TRUE);#3-party package to carry out excel type lookups 
install.packages("networkD3", dependencies = TRUE);
install.packages("sqldf", dependencies = TRUE);
install.packages("Rtools", dependencies = TRUE);
install.packages("lattice", dependencies = TRUE);#3-party package to generate comparitive plots as lattices 
install.packages("quantmod", dependencies = TRUE);#3-party package Quantitative Financial Modelling Framework for quantitiative work
install.packages("functional", dependencies = TRUE);#3-party package adding new columns with calculated values
install.packages("Deducer", dependencies = TRUE); #alternative viewer to RStudio
install.packages("forecast", dependencies = TRUE); #alternative viewer to RStudio
install.packages("latticeExtra", dependencies = TRUE);#3-party package to generate comparitive plots as lattices 
install.packages("tseries", dependencies = TRUE);#3-party package to generate comparitive plots as lattices 
install.packages("timeSeries", dependencies = TRUE);#3-party package to generate comparitive plots as lattices 
#install.packages("grDevices");#3-party package to fix base plots axis -- package ‘grDevices’ is not available (for R version 3.2.2)
install.packages("timeDate", dependencies = TRUE);#3-party package to work on date and time objects
install.packages("openxlsx", dependencies = TRUE);#3-party package that Simplifies the creation of Excel .xlsx files by providing a 
#high level interface to writing, styling and editing worksheets. Through
#the use of Rcpp, read/write times are comparable to the xlsx and XLConnect
#packages with the added benefit of removing the dependency on Java
install.packages("TSA", dependencies = TRUE);
install.packages("extraDistr", dependencies = TRUE);#3-party package containing various additional probability distributions
install.packages("creditr", dependencies = TRUE);#3-party package to price credit default swaps
install.packages("credule", dependencies = TRUE);#3-party package to price credit default swaps
install.packages("RQuantLib", dependencies = TRUE);#3-party package to price credit default swaps
install.packages("copula", dependencies = TRUE);#3-party package to price credit default swaps
install.packages("doMC", dependencies = TRUE);#3-party package to price credit default swaps
install.packages("neuralnet", dependencies = TRUE);#3-party package to do neural network based data and model analysis
install.packages("tseries", dependencies = TRUE);
install.packages("fImport", dependencies = TRUE);
install.packages("rdatamarket", dependencies = TRUE);
install.packages("fImport", dependencies = TRUE);
install.packages("pdfetch", dependencies = TRUE);
install.packages("Quandl", dependencies = TRUE);
install.packages("quantmod", dependencies = TRUE);
install.packages("xtable", dependencies = TRUE);
install.packages("gdata", dependencies = TRUE);
install.packages("plotly", dependencies = TRUE);
install.packages("vars", dependencies = TRUE); #try  to  fill  a  gap  in  the  econometrics’  methods landscape of R 
#by providing the “standard” tools in the context of VAR, SVAR and SVEC analysis
install.packages("coinmarketcapr", dependencies = TRUE);#3-party package to pull data from coin market cap 
                                                        ##for cryptocureencies
install.packages("formatR", dependencies = TRUE);#3-party package to visualize data from coin market cap 
install.packages("yaml", dependencies = TRUE);#3-party package to visualize data from coin market cap 
install.packages("googleVis", dependencies = TRUE);#3-party package to visualize data from coin market cap 
install.packages("knitr", dependencies = TRUE);#3-party package to visualize data from coin market cap 
install.packages("RCurl", dependencies = TRUE);#3-party package to visualize data from coin market cap 
install.packages("httr", dependencies = TRUE);#3-party package to visualize data from coin market cap 
install.packages("dse", dependencies = TRUE);
install.packages("fArma", dependencies = TRUE);#(Ẅurtz 2007) are made available for estimating ARIMA and VARIMA time series models
install.packages("MSBVAR", dependencies = TRUE);#(Brandt and Appleby 2007) provides methods for estimating frequentist 
#and Bayesian vector autoregression (BVAR) models
install.packages("janitor");##used for clean up formating and especially when the ns_to_percents function is called
                            ##Convert a numeric data.frame to row-,  column-,  or totals-wise percentages.
                            ##  
install.packages("rjags", dependencies = TRUE); ##Used in tandom with JAGs to do baysian statistics
install.packages("rstan", repos = "https://cloud.r-project.org/", dependencies=TRUE);
install.packages("rstan", repos = "https://cloud.r-project.org/", dependencies=TRUE)
install.packages("bayesTFR", dependencies = TRUE); ##UN package for population projections (computes fertility rates)
install.packages("bayesLife", dependencies = TRUE); ##UN package for population projections (computes life expectancy rates)
install.packages("bayesPop", dependencies = TRUE); ##UN package for population projections (computes population pojections)
install.packages("bayesDem", dependencies = TRUE); ##UN package for population projections (The graphic user interface)
install.packages("wppExplorer", dependencies = TRUE); ##UN package for population projections (The graphic user interface)
install.packages("MCMCpack", dependencies = TRUE);
install.packages("arm", dependencies = TRUE);
install.packages("rlang", dependencies=TRUE);
install.packages("shiny", dependencies=TRUE);
install.packages("bayesplot", dependencies=TRUE);
require(devtools)
install_version("backports", version = "1.1.0")
require(bayesplot);
install.packages("shinystan", dependencies=TRUE);
install.packages('lmerTest', dependencies = TRUE)
install.packages("migest", dependencies = TRUE);
install.packages("backports", dependencies = TRUE);
install.packages("psych", dependencies = TRUE);
install.packages("ggpubr", dependencies = TRUE);
install.packages("tidyverse", dependencies = TRUE);
install.packages("tibble", dependencies = TRUE);
install.packages("Hmisc", dependencies = TRUE);
install.packages("corrplot", dependencies = TRUE);
#install.packages("Rtools");
install.packages('devtools', dependencies = TRUE);##Install the Have you look looked at dse: Dynamic Systems Estimation? 
							 ##In addition to the standard ARMA model it supports VAR and VARX analysis.
library('devtools');
install_github('fastVAR', user='jeffwong');
##For fast Bayesian analysis
library('devtools');
install_github('rmcelreath/glmer2stan');

source_url("https://github.com/stan-dev/shinystan/raw/develop/install_shinystan.R");
install_shinystan();

