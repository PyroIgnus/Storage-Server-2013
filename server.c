/**
 * @file
 * @brief This file implements the storage server.
 *
 * The storage server should be named "server" and should take a single
 * command line argument that refers to the configuration file.
 * 
 * The storage server should be able to communicate with the client
 * library functions declared in storage.h and implemented in storage.c.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include "utils.h"
#include "storage.h"
#include <time.h>
#define MAX_LISTENQUEUELEN 20	///< The maximum number of queued connections.
#include "file.h"

//double total_processing_time;

/**
 * @brief Process a command from the client.
 *
 * @param sock The socket connected to the client.
 * @param cmd The command received from the client.
 * @param cmdidenfify The command identification.
 * @param cmdusername The username received from the client.
 * @param cmdtable The table received from the client.
 * @param cmdkey The key received from the client.
 * @param cmdvalue The value received from the client.
 * @return Returns 0 on success, -1 otherwise.
 */
int handle_command(int sock, char *cmd,struct config_params *params, struct table* head )
{

	char cmdidentify[20];
	char cmdusername[MAX_USERNAME_LEN];
	char cmdpassword[MAX_ENC_PASSWORD_LEN];
	char cmdtable[MAX_TABLE_LEN];
	char cmdkey[MAX_KEY_LEN];
	char cmdvalue[MAX_VALUE_LEN];
	int success = 0;


	sscanf(cmd,"%s",cmdidentify);
	char buff[50];
	sprintf(buff,"Processing command '%s'\n", cmd);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);

	if (strcmp (cmdidentify, "AUTH") == 0){
		sscanf(cmd,"%*s %s %s",cmdusername,cmdpassword);
		if (strcmp (cmdusername,params->username)!=0 || strcmp (cmdpassword,params->password)!=0){
			//set the errno as well

			return -1;
		}
	}
	else if (strcmp (cmdidentify, "GET") == 0) {

		struct timeval start_time, end_time;
        // Remember when the experiment started.
		gettimeofday(&start_time);

		sscanf(cmd,"%*s %s %s",cmdtable,cmdkey);
		success = getEntry(head, cmdtable, cmdkey);
		//Error Entry not found
		if (success == -1) {
			errno_test=ERR_TABLE_NOT_FOUND;
			return success;
		}
		else if (success == -2) {
			errno_test=ERR_KEY_NOT_FOUND;
			return success;
		}
		else
			sprintf (cmd, "%d", success);

		// Get the time at the end of the experiment.
		gettimeofday(&end_time);

		//get difference in time
		//total_processing_time+=getdifftime(&start_time, &end_time);

		success = 0;
	}
	else if (strcmp (cmdidentify, "SET") == 0) {

		struct timeval start_time, end_time;
        // Remember when the experiment started.
		gettimeofday(&start_time);

		sscanf(cmd,"%*s %s %s %s",cmdtable,cmdkey, cmdvalue);
		success = setEntry(head, cmdtable, cmdkey, cmdvalue);
		if(success==-1){
			errno_test=ERR_TABLE_NOT_FOUND;
		}

		// Get the time at the end of the experiment.
		gettimeofday(&end_time);

		//get difference in time
		//total_processing_time+=getdifftime(&start_time, &end_time);

	}

//	printf("The total processing time is %d.\n",total_processing_time);

	// For now, just send back the command to the client.
	sendall(sock, cmd, strlen(cmd));
	sendall(sock, "\n", 1);

	return success;
}

/**
 * @brief Start the storage server.39
 *
 * This is the main entry point for the storage server.  It reads the
 * configuration file, starts listening on a port, and proccesses
 * commands from clients.
 */
int main(int argc, char *argv[])
{
	// Initialize table linked list
	char buff [100];
	if (LOGGING==2){
		time_t rawtime;
		struct tm * timeinfo;
		time ( &rawtime );
		timeinfo = localtime ( &rawtime );
		strftime (buff,80,"Server-%Y-%m-%d-%H-%M-%S.log",timeinfo);
		file=fopen(buff,"a");
	}
	assert(argc > 0);
	if (argc != 2) {
		sprintf(buff, "Usage %s <config_file>\n", argv[0]);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}
	char *config_file = argv[1];

	// Read the config file.
	struct config_params params;
	//initialise the values for flags in params
	params.server_host_exist=0;
	params.server_port_exist=0;
	params.username_exist=0;
	params.password_exist=0;
	params.tableIndex=0;
	int status = read_config(config_file, &params);
	if (status != 0) {
		sprintf(buff,"Error processing config file.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}
//	// Initialize the tables in database
	struct table *head;
	struct table *curr;



	head=malloc(sizeof(struct table));
	head->next=NULL;
	strcpy(head->name,params.table_name[0]);
	curr=head;
	int i, j;
	for (i=1;i<params.tableIndex;i++){
		curr->next=malloc(sizeof(struct table));
		curr=curr->next;
		strcpy(curr->name,params.table_name[i]);
	}
	//make the last node point to NULL
	curr->next=NULL;

	//move curr to head again
	curr=head;

	int numTables = params.tableIndex ;
	for (i = 0; i < numTables; i++) {
		for (j = 0; j < MAX_RECORDS_PER_TABLE;j++) {
			curr->entries[j] = malloc(sizeof(struct hashEntry));
			curr->entries[j]->deleted = -1;
		}
		curr = curr->next;
	}
	curr = head;




	sprintf(buff,"Server on %s:%d\n", params.server_host, params.server_port);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	// Create a socket.
	int listensock = socket(PF_INET, SOCK_STREAM, 0);
	if (listensock < 0) {
		sprintf(buff,"Error creating socket.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}
	// Allow listening port to be reused if defunct.
	int yes = 1;
	status = setsockopt(listensock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
	if (status != 0) {
		sprintf(buff,"Error configuring socket.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}

	// Bind it to the listening port.
	struct sockaddr_in listenaddr;
	memset(&listenaddr, 0, sizeof listenaddr);
	listenaddr.sin_family = AF_INET;
	listenaddr.sin_port = htons(params.server_port);
	inet_pton(AF_INET, params.server_host, &(listenaddr.sin_addr)); // bind to local IP address
	status = bind(listensock, (struct sockaddr*) &listenaddr, sizeof listenaddr);
	if (status != 0) {
		sprintf(buff,"Error binding socket.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}
	// Create a socket.
	// Listen for connections.
	status = listen(listensock, MAX_LISTENQUEUELEN);
	if (status != 0) {
		sprintf(buff,"Error listening on socket.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}

	// Listen loop.
	int wait_for_connections = 1;
	while (wait_for_connections) {
		// Wait for a connection.
		struct sockaddr_in clientaddr;
		socklen_t clientaddrlen = sizeof clientaddr;
		int clientsock = accept(listensock, (struct sockaddr*)&clientaddr, &clientaddrlen);
		if (clientsock < 0) {
			sprintf(buff,"Error accepting a connection.\n");
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			exit(EXIT_FAILURE);
		}
		char buff[50];
		sprintf(buff,"Got a connection from %s:%d.\n", inet_ntoa(clientaddr.sin_addr), clientaddr.sin_port);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		// Get commands from client.
		int wait_for_commands = 1;
		do {
			// Read a line from the client.
			char cmd[MAX_CMD_LEN];
			int status = recvline(clientsock, cmd, MAX_CMD_LEN);
			if (status != 0) {
				// Either an error occurred or the client closed the connection.
				wait_for_commands = 0;
			} else {
				// Handle the command from the client.
				int status = handle_command(clientsock, cmd, &params, head);
				if (status != 0)
					wait_for_commands = 0; // Oops.  An error occured.
			}
		} while (wait_for_commands);

		// Close the connection with the client.
		close(clientsock);
		sprintf(buff,"Closed connection from %s:%d.\n", inet_ntoa(clientaddr.sin_addr), clientaddr.sin_port);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
	}

	// Stop listening for connections.
	close(listensock);
	fclose(file);

	return EXIT_SUCCESS;
}

