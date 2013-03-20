/**
 * @file
 * @brief This file defines the filestream that would be used in the client and server sides,
 *  as well as the method of output.
 *
 */
#ifndef FILE_H_
#define FILE_H_
#include "file.h"
#include <stdio.h>

///this controls the output stream of thext; when it is 1, the data is output on shell.
///when 2, the data gets output on a filestream. No data is outputted in case it is 0
/// Create a socket.ocpublicket connected to the client.

#define LOGGING 0
///this is the filestream that would be used by server and client
FILE *file;


#endif /* FILE_H_ */
