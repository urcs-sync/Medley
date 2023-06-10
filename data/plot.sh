#!/bin/bash
# go to where plot.sh locates
cd "$( dirname "${BASH_SOURCE[0]}" )"

# Figure 7
Rscript hts_plotting.R

# Figure 8
Rscript sls_plotting.R

# Figure 9
Rscript tpcc_plotting.R

# Figure 10
Rscript sls_latency_plotting.R