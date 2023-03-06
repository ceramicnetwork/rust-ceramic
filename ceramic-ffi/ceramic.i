%module example
%{
/* Includes the header in the wrapper code */
#include "ceramic.h"
%}

/* Parse the header file to generate wrappers */
%include "ceramic.h"
