#!/bin/bash

# eliminate the begining key value from the hadoop output
# and insure we have unique values to prevent duplicate
# primary keys

for f in contacts documents entities filings filings_entities owner_rels; do
    echo "Processing $f:"
    sed -n -e "/^$f/s@[^|]*|@@p" $*  | sort -u  >$f.csv
    # Explanation:
    # sed:
    # -n, --quiet, --silent: suppress automatic printing of pattern space
    # -e script, --expression=script: add the script to the commands to be executed
    # The general form is "sed -e '/pattern/ command' my_file"
    #   s => search & replace
    #   p => print
    #   d => delete
    #   i => insert
    #   a => append
    # In the pattern above, @ is being used as the delimiter. So what you really have is:
    # /^$f/  => find the current pattern ("contacts", "documents", etc.)
    # s      => substitute
    # [^|]*| => 0 or more characters other than pipe (|), followed by a pipe, with
    #        => (nothing)
    # p      => print the result
    #
    # The effect is to remove the first field but leave the rest of the data intact
    #
    # $* represents the positional arguments in bash (in this case, the files to be processed)
    # |  redirects the output to the next command
    #
    # sort:
    # -u, --unique (with -c, check for strict ordering; without -c, output only the first of an equal run)
    #
    # >$f.csv => print line to "contacts", "documents", etc. and tack on a .csv extension
    #
    # The overall effect is to split the pipe-delimited filing data into separate files based on what kind of data it is.
done

