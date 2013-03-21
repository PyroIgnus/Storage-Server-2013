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
#include <sys/time.h>
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
int handle_command(int sock, char *cmd, struct config_params *params, struct table* head )
{

	char cmdidentify[20];
	char cmdusername[MAX_USERNAME_LEN];
	char cmdpassword[MAX_ENC_PASSWORD_LEN];
	char cmdtable[MAX_TABLE_LEN];
	char cmdkey[MAX_KEY_LEN];
	char cmdvalue[MAX_VALUE_LEN];
	char result[MAX_VALUE_LEN] = "";
	int success = 0;
	int maxKeys = 0;
	char* arg;


	sscanf(cmd,"%s",cmdidentify);
	char buff[50];
	sprintf(buff,"Processing command '%s'\n", cmd);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);

	if (strcmp (cmdidentify, "AUTH") == 0){
		sscanf(cmd,"%*s %s %s",cmdusername,cmdpassword);
		if (strcmp (cmdusername,params->username)!=0 || strcmp (cmdpassword,params->password)!=0){
			//set the errno as well
			sprintf (cmd, "-1");

			success = -1;
		}
	}
	else if (strcmp (cmdidentify, "GET") == 0) {

		struct timeval start_time, end_time;
        // Remember when the experiment started.
		gettimeofday(&start_time,NULL);
		double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

		sscanf(cmd,"%*s %s %s",cmdtable,cmdkey);
		strcpy (result, getEntry(head, cmdtable, cmdkey));
		success = atoi (result);
		//Error -1 = Table not found, -2 = Key not found
		if (success < 0) {
			sprintf (cmd, "%d", success);
		}
		else {



			//sprintf (cmd, "%[0-9a-zA-Z ,]", result);
			sprintf (cmd, "%s", result);
		}

		// Get the time at the end of the experiment.
		gettimeofday(&end_time,NULL);
		double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

		//get difference in time
		double total_processing_time=t2-t1;



		sprintf(buff, "The server processing time was %.6lf seconds.\n",total_processing_time);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);


		success = 0;
	}
	else if (strcmp (cmdidentify, "SET") == 0) {

		struct timeval start_time, end_time;
        	// Remember when the experiment started.
		gettimeofday(&start_time,NULL);
		double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);
//		sscanf(cmd,"%*s %s %s %s",cmdtable,cmdkey, cmdvalue);
		arg = strtok (cmd, " ");
		arg = strtok (NULL, " ");
		strcpy (cmdtable, arg);
		arg = strtok(NULL, " ");
		strcpy (cmdkey, arg);
		//Place the rest into cmdvalue.
		arg = strtok(NULL, " ");
		arg[strlen(arg)] = ' ';
		strcpy (cmdvalue, arg);


		//char *buff2;
		//strcpy(buff2,cmdvalue);
		success = setEntry(head, cmdtable, cmdkey, cmdvalue);
		//Error -1 = Table not found, -2 = Wrong column format/Invalid param
		if(success < 0){
			sprintf (cmd, "%d", success);
		}

		// Get the time at the end of the experiment.
		gettimeofday(&end_time,NULL);
		double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

		//get difference in time
		double total_processing_time=t2-t1;


		sprintf(buff,"The server processing time was %.6lf seconds.\n",total_processing_time);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
	}
	else if (strcmp (cmdidentify, "FILE") == 0) {
		struct timeval start_time, end_time;
		gettimeofday(&start_time,NULL);
		double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

		sscanf(cmd,"%*s %s",cmdtable);
		char TABLE[MAX_TABLE_LEN];
		char KEY[MAX_KEY_LEN];
		char TEMP[100];
		char values[MAX_COLUMNS_PER_TABLE][MAX_VALUE_LEN];
		char value[MAX_VALUE_LEN];
		int status = 0;

		strcpy(TABLE,cmdtable);

		FILE *infile;
		infile=fopen(cmdtable,"r");
		sprintf(buff, "Opening file %s.\n", cmdtable);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		if (infile != NULL) {
			while(fscanf(infile,"%s",&TEMP)!=EOF) {
				if (strlen(TEMP)>19) {
					memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
					KEY[MAX_KEY_LEN - 1] = '\0';
				}
				else {
					strcpy(KEY,TEMP);
				}
				fscanf(infile,"\t%s\t%s\t%s\n",&values[0], &values[1], &values[2]);


				strcpy (value, "");
				strcat (value, "testchar ");
				strcat (value, trim(values[0]));
				strcat (value, ", int1 ");
				strcat (value, trim(values[1]));
				strcat (value, ", int2 ");
				strcat (value, trim(values[2]));


				char *buff2;
				strcpy(buff2,value);
				success = setEntry(head, TABLE, KEY, buff2);


				if (LOGGING != 0)
					sprintf(buff, "stored for %s %s\n",KEY,value);
				if (LOGGING == 1) logger(stdout, buff);
				else if (LOGGING == 2) logger(file, buff);

			}
		}
		else {
			sprintf(buff,"File not opened successfully.\n");
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			sprintf (cmd, "%d", success);
		}


//	Original M2 File Parser for workload testing.
//		sscanf(cmd,"%*s %s",cmdtable);
//		char TABLE[MAX_TABLE_LEN];
//		char KEY[MAX_KEY_LEN];
//		char TEMP[100];
//		char value[MAX_VALUE_LEN];
//		int status = 0;
//
//		strcpy(TABLE,cmdtable);
//
//		FILE *infile;
//		infile=fopen(cmdtable,"r");
//		sprintf(buff, "Opening file %s.\n", cmdtable);
//		if (LOGGING == 1) logger(stdout, buff);
//		else if (LOGGING == 2) logger(file, buff);
//		if (infile != NULL) {
//			while(fscanf(infile,"%s",&TEMP)!=EOF) {
//				if (strlen(TEMP)>19) {
//					memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
//					KEY[MAX_KEY_LEN - 1] = '\0';
//				}
//				else {
//					strcpy(KEY,TEMP);
//				}
//				fscanf(infile,"\t%s\n",&value);
//
//				success = setEntry(head, TABLE, KEY, value);
//				if (LOGGING != 0)
//					sprintf(buff, "stored for %s %s\n",KEY,value);
//				if (LOGGING == 1) logger(stdout, buff);
//				else if (LOGGING == 2) logger(file, buff);
//
//			}
//		}
//		else {
//			sprintf(buff,"File not opened successfully.\n");
//			if (LOGGING == 1) logger(stdout, buff);
//			else if (LOGGING == 2) logger(file, buff);
//			sprintf (cmd, "%d", success);
//		}

		// Get the time at the end of the experiment.
		gettimeofday(&end_time,NULL);
		double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

		//get difference in time
		double total_processing_time=t2-t1;
;
		printf("The server processing time was %.6lf seconds.\n",total_processing_time);
		sprintf(buff,"The server processing time was %.6lf seconds.\n",total_processing_time);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
	}
	else if (strcmp (cmdidentify, "QUERY") == 0) {
		struct timeval start_time, end_time;
        	// Remember when the experiment started.
		gettimeofday(&start_time,NULL);
		double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

		//sscanf(cmd,"%*s %s %d %s", cmdtable, maxKeys, cmdvalue);
		arg = strtok (cmd, " ");
		arg = strtok (NULL, " ");
		strcpy (cmdtable, arg);
		arg = strtok (NULL, " ");
		maxKeys = atoi (arg);
		//Place the rest into cmdvalue.
		arg = strtok(NULL, " ");
		arg[strlen(arg)] = ' ';
		strcpy (cmdvalue, arg);

		strcpy (cmd, query(head, cmdtable, cmdvalue, maxKeys));

		if (atoi(cmd) >= 0)
			sprintf (buff, "Keys found: %s\n", cmd);
		else
			sprintf (buff, "Sending Error Message: %d\n", atoi(cmd));
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);

		// Get the time at the end of the experiment.
		gettimeofday(&end_time,NULL);
		double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

		//get difference in time
		double total_processing_time=t2-t1;

		//printf("The total time in client side is %.6lf seconds.\n",total_processing_time);
		printf("The server processing time was %.6lf seconds.\n",total_processing_time);
		sprintf(buff,"The server processing time was %.6lf seconds.\n",total_processing_time);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
	}

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
	int i, j;

	head=malloc(sizeof(struct table));
	// Needs proper implementation from config parsing.
	head->numCol = params.numCol[0];
	head-> headIndex = -1;
	head->numEntries = 0;
	head->next=NULL;
	strcpy(head->name,params.table_name[0]);
	for (i = 0; i < head->numCol; i++) {
		strcpy(head->col[i], params.col[0][i]);
		head->type[i] = params.type[0][i];
	}
	curr=head;
	for (i=1;i<params.tableIndex;i++){
		curr->next=malloc(sizeof(struct table));
		curr=curr->next;
		curr-> headIndex = -1;
		curr->numEntries = 0;
		strcpy(curr->name,params.table_name[i]);
		// Needs implementation of config parsing to determine a valid number.  For now set to 0.
		curr->numCol = params.numCol[i];
		for (j = 0; j < curr->numCol; j++) {
			strcpy(curr->col[j], params.col[i][j]);
			curr->type[j] = params.type[i][j];
		}

	}
	//make the last node point to NULL
	curr->next=NULL;

	//move curr to head again
	curr=head;

	int numTables = params.tableIndex ;
	for (i = 0; i < numTables; i++) {
		for (j = 0; j < MAX_RECORDS_PER_TABLE;j++) {
			curr->entries[j] = malloc(sizeof(struct hashEntry));
			curr->entries[j]->next = NULL;
			curr->entries[j]->prev = NULL;
			curr->entries[j]->deleted = -1;
		}
		curr = curr->next;
	}
	curr = head;




// To test the table linked list

//	while(curr!=NULL){
//		printf("%s \n", curr->name);
//		int i=curr->numCol;
//		int j;
//		for (j=0;j<i;j++){
//			if (curr->type[j]==-1)
//				printf("\tint %s ",curr->col[j]);
//			else
//				printf("\tchar[%d] %s",(int)curr->type[j],curr->col[j]);
//			printf("\n");
//		}
////        printf("%s\n\n", curr->col[0]);
//		curr=curr->next;
//	}












/*

	//Initial load of census workload

	head->numCol = 1;

	char TABLE[MAX_TABLE_LEN];
	char KEY[MAX_KEY_LEN];
	char TEMP[100];
	char value[MAX_VALUE_LEN];
	struct timeval start_time, end_time;

	sprintf(buff, "Initializing workload.\n");
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);

	strcpy(TABLE,"census");

	FILE *infile;
	infile=fopen("census","r");

	// Remember when the experiment started.
	gettimeofday(&start_time,NULL);
	double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

	while(fscanf(infile,"%s",&TEMP)!=EOF) {
		if (strlen(TEMP)>19) {
			memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
			KEY[MAX_KEY_LEN - 1] = '\0';
		}
		else {
			strcpy(KEY,TEMP);
		}
		fscanf(infile,"\t%s\n",&value);

		status = setEntry(head, TABLE, KEY, value);
		if (LOGGING != 0)
			sprintf(buff, "stored for %s %s\n",KEY,value);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);

	}

	gettimeofday(&end_time,NULL);
	double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

	//get difference in time
	double total_processing_time=t2-t1;

	printf ("The total time is %.6lf seconds.\n",total_processing_time);	
	sprintf(buff, "The total time is %.6lf seconds.\n",total_processing_time);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);

*/


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
				if (!(cmd == NULL || strlen(cmd) < 2)) {
					// Handle the command from the client.
					int status = handle_command(clientsock, cmd, &params, head);
				}
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
	
	// Free memory
	freeTable (head);

	return EXIT_SUCCESS;
}

