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
#include <sys/time.h>
#include <errno.h>
/**
 * @brief for error checking.
 */
int errno;
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
	sprintf(buff, "%d", port);
	printf ("%s", buff);
	// Assume port is given in the correct format.
	if (my_strvalidate(hostname, 3)) {
		errno=ERR_INVALID_PARAM;
		return NULL;
	}

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
	if (!conn) {
		errno = ERR_CONNECTION_FAIL;
		return -1;
	}

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
		if (strcmp (buf, "-1") == 0) {
			auth = 0;
			errno = ERR_AUTHENTICATION_FAILED;
			return -1;
		}
		auth=1;
		return 0;
	}
	auth=0;
	errno=ERR_AUTHENTICATION_FAILED;    //error :authentication failed
	return -1;
}

/**
 * @brief This is used to get values back from the database
 */
int storage_get(const char *table, const char *key, struct storage_record *record, void *conn)
{
	if (!conn) {
		errno = ERR_CONNECTION_FAIL;
		return -1;
	}
	if (strlen(table)>20){
		errno=ERR_INVALID_PARAM;
		return -1;
	}
	if (my_strvalidate(table, 1) ||my_strvalidate(key, 1)){
		errno=ERR_INVALID_PARAM;
		return -1;
	}


	struct timeval start_time, end_time;

	// Remember when the experiment started.
	gettimeofday(&start_time,NULL);
	double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

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
			if (strcmp (buf, "-1") == 0) {
				errno = ERR_TABLE_NOT_FOUND;
				return -1;
			}
			if (strcmp (buf, "-2") == 0) {
				errno = ERR_KEY_NOT_FOUND;
				return -1;
			}
				
			strncpy(record->value, buf, sizeof record->value);

			// Get the time at the end of the experiment.
			gettimeofday(&end_time,NULL);
			double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

			// Calculate difference in time and output it.
		    	double totaltime=t2-t1;



			sprintf(buff,"The total time in client side is %.6lf seconds.\n",totaltime);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);


			return 0;
		}
	}
	else{
		errno=ERR_NOT_AUTHENTICATED;    //error :not authenticated
		return -1;
	}
	// Get the time at the end of the experiment.

	// Calculate difference in time and output it.


	//errno=errno_test;    //error :table or key not found
	return -1;

}


/**
 * @brief This is used to set data into database
 */
int storage_set(const char *table, const char *key, struct storage_record *record, void *conn)
{

	char TEMP[100];

	if (!conn) {
		errno = ERR_CONNECTION_FAIL;
		return -1;
	}

	if (strlen(key)>MAX_KEY_LEN - 1){
		memcpy( TEMP, &key, MAX_KEY_LEN - 1);
		TEMP[MAX_KEY_LEN - 1] = '\0';
	}
	else {
		strcpy(TEMP,key);
	}

	if (my_strvalidate(table, 1) ||my_strvalidate(key, 1)){
		errno=ERR_INVALID_PARAM;
		return -1;
	}

	struct timeval start_time, end_time;

	// Remember when the experiment started.
	gettimeofday(&start_time,NULL);
	double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

	char buff[100];
	//print statement here will tell us if it started setting the value to table
	sprintf(buff, "Accessing table '%s' to set value to key '%s'\n",
			table, TEMP);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	// Connection is really just a socket file descriptor.
	int sock = (int)conn;

	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	if (strcmp (record->value, "FILESTORE") == 0)
		snprintf(buf, sizeof buf, "FILE %s \n", table);
	else
		snprintf(buf, sizeof buf, "SET %s %s %s\n", table, TEMP, record->value);

	if(auth==1){
//	    printf(" sending data \n");
		if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0) {

			if (strcmp (buf, "-1") == 0) {
				errno = ERR_TABLE_NOT_FOUND;
				return -1;
			}
			if (strcmp (buf, "-2") == 0) {
				errno = ERR_UNKNOWN;
				return -1;
			}
			// Get the time at the end of the experiment.
			gettimeofday(&end_time,NULL);
			double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

			// Calculate difference in time and output it.
		    	double totaltime=t2-t1;


			printf("The total time in client side is %.6lf seconds.\n",totaltime);
			sprintf(buff,"The total time in client side is %.6lf seconds.\n",totaltime);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);



			// Calculate difference in time and output it.


			return 0;
		}
	}
	else{

		printf(" authentication error \n");

		errno=ERR_NOT_AUTHENTICATED;    //error :not authenticated
		return -1;
	}
	// Get the time at the end of the experiment.

	// Calculate difference in time and output it.


    //errno=errno_test;   //key not found
	return -1;
}

int storage_query(const char *table, const char *predicates, char **keys, const int max_keys, void *conn) {
	printf ("Request query.\n");
	struct timeval start_time, end_time;

	// Remember when the experiment started.
	gettimeofday(&start_time,NULL);
	double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

	char buff[100];
	//print statement here will tell us if it started accessing the table
	sprintf(buff, "Accessing table '%s' to fetch keys from predicates: '%s'\n",
			table, predicates);
	if (LOGGING == 0) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	// Connection is really just a socket file descriptor.

	int sock = (int)conn;


	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "QUERY %s %s %d\n", table, predicates, max_keys);
	if(auth==1){
		if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0) {

			// Take buf and parse it for the number of keys and the list of keys and separate them into their appropriate variables (return number and set list into keys).

			// Get the time at the end of the experiment.
			gettimeofday(&end_time,NULL);
			double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

			// Calculate difference in time and output it.
		    	double totaltime=t2-t1;



			sprintf(buff,"The total time in client side is %.6lf seconds.\n",totaltime);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);


			return 0;
		}
	}
	else{
		errno=ERR_NOT_AUTHENTICATED;    //error :not authenticated
		return -1;
	}
	// Get the time at the end of the experiment.

	// Calculate difference in time and output it.


	//errno=errno_test;    //error :table or key not found
	return -1;
}


/**
 * @brief This is used to disconnect the connection
 */
int storage_disconnect(void *conn)
{
	if (!conn) {
		errno = ERR_INVALID_PARAM;
		return -1;
	}
	// Cleanup
	int sock = (int)conn;
	if (sock < 0){
		errno=ERR_INVALID_PARAM;    //error :invalid
		return -1;
	}
	auth = 0;
	int status = close(sock);
	if (status == -1) {
		errno = ERR_UNKNOWN;
		return -1;
	}
	return 0;
}

