/**
 * @file
 * @brief This file contains the implementation of the storage server
 * interface as specified in storage.h.
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include "storage.h"
#include "utils.h"
#include "file.h"
/**
 * @brief for error checking.
 */
int errno=0;
/**
 * @brief helper variable for checking authorisation.
 */
int auth;


/**
 * @brief This is used to establish a connection with server.
 */
void* storage_connect(const char *hostname, const int port)
{
	char buff[100];
	//print statement here will tell us if it started connecting to the server or not
	sprintf(buff, "Attempting to connect to server '%s'\n",
			hostname);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	// Create a socket.
	int sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock < 0){
		errno=ERR_INVALID_PARAM;    //error :invalid
		return NULL;
	}
	// Get info about the server.
	struct addrinfo serveraddr, *res;
	memset(&serveraddr, 0, sizeof serveraddr);
	serveraddr.ai_family = AF_UNSPEC;
	serveraddr.ai_socktype = SOCK_STREAM;
	char portstr[MAX_PORT_LEN];
	snprintf(portstr, sizeof portstr, "%d", port);
	int status = getaddrinfo(hostname, portstr, &serveraddr, &res);
	if (status != 0){
		errno=ERR_INVALID_PARAM;    //error :invalid
		return NULL;
	}
	// Connect to the server.
	status = connect(sock, res->ai_addr, res->ai_addrlen);
	if (status != 0){
		errno=ERR_CONNECTION_FAIL;    //error :connection fialed
		return NULL;
	}
	return (void*) sock;
}


/**
 * @brief This is used to authenticate the arguments from the client
 */
int storage_auth(const char *username, const char *passwd, void *conn)
{
	char buff[100];
	//print statement here will tell us if it started logging in
	sprintf(buff, "Attempting to Login with username  '%s'\n",username);

	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	// Connection is really just a socket file descriptor.
	int sock = (int)conn;

	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	char *encrypted_passwd = generate_encrypted_password(passwd, NULL);
	snprintf(buf, sizeof buf, "AUTH %s %s\n", username, encrypted_passwd);
	if (sendall(sock, buf, strlen(buf)) ==  0 && recvline(sock, buf, sizeof buf) == 0) {
		auth=1;
		return 0;
	}
	auth=1;
	errno=ERR_AUTHENTICATION_FAILED;    //error :authentication failed
	return -1;
}

/**
 * @brief This is used to get values back from the database
 */
int storage_get(const char *table, const char *key, struct storage_record *record, void *conn)
{
	struct timeval start_time, end_time;

	// Remember when the experiment started.
	gettimeofday(&start_time);

	char buff[100];
	//print statement here will tell us if it started accessing the table
	sprintf(buff, "Accessing table '%s' to fetch value from key '%s'\n",
			table, key);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	// Connection is really just a socket file descriptor.

	int sock = (int)conn;


	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "GET %s %s\n", table, key);
	if(auth==1){
		if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0) {
			strncpy(record->value, buf, sizeof record->value);

			// Get the time at the end of the experiment.
			gettimeofday(&end_time);

			// Calculate difference in time and output it.
			printdifftime(&start_time, &end_time);

			return 0;
		}
	}
	else{
		errno=ERR_NOT_AUTHENTICATED;    //error :not authenticated
		return -1;
	}
	// Get the time at the end of the experiment.
	gettimeofday(&end_time);

	// Calculate difference in time and output it.
	printdifftime(&start_time, &end_time);

	errno=errno_test;    //error :table or key not found
	return -1;

}


/**
 * @brief This is used to set data into database
 */
int storage_set(const char *table, const char *key, struct storage_record *record, void *conn)
{

	struct timeval start_time, end_time;

	// Remember when the experiment started.
	gettimeofday(&start_time);

	char buff[100];
	//print statement here will tell us if it started setting the value to table
	sprintf(buff, "Accessing table '%s' to set value to key '%s'\n",
			table, key);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	// Connection is really just a socket file descriptor.
	int sock = (int)conn;

	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "SET %s %s %s\n", table, key, record->value);

	if(auth==1){
//	    printf(" sending data \n");
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0) {

//		printf(" received data \n");


		// Get the time at the end of the experiment.
		gettimeofday(&end_time);

		// Calculate difference in time and output it.
		printdifftime(&start_time, &end_time);

		return 0;
	}
	}
	else{

		printf(" authentication error \n");

		errno=ERR_NOT_AUTHENTICATED;    //error :not authenticated
		return -1;
	}
	// Get the time at the end of the experiment.
	gettimeofday(&end_time);

	// Calculate difference in time and output it.
	printdifftime(&start_time, &end_time);

    errno=errno_test;   //key not found
	return -1;
}


/**
 * @brief This is used to disconnect the connection
 */
int storage_disconnect(void *conn)
{
	// Cleanup
	int sock = (int)conn;
	close(sock);

	return 0;
}

