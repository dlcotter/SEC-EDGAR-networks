#!/bin/bash

# read transactions table and generate connections

awk -F \| 'BEGIN {issuerCIK = ""; prevOwner = "";}

{
    if ( $1 != issuerCIK ) {
        issuerCIK = $1;
        delete cons;
    }
    ownerCIK = $3;
    if ( ownerCIK in cons ) {
        # printf("ownerCIK: %s\n",ownerCIK );
        for (other in cons[ownerCIK]) {
            if ( other != ownerCIK ) {
                if ( other < ownerCIK ) {
                    printf("%s|%s|%s\n",issuerCIK,other,ownerCIK );
                } else {
                    printf("%s|%s|%s\n",issuerCIK,ownerCIK,other );
                }
                delete cons[ownerCIK][other];
            }
        }
    }
    else {
        for ( key in cons ) {
            cons[key][ownerCIK] = 1;
        }
        cons[ownerCIK][ownerCIK] = 1;
    }
}' $*

