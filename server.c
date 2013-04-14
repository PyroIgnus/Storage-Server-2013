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
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>
#define MAX_LISTENQUEUELEN 20	///< The maximum number of queued connections.
#include "file.h"

pthread_mutex_t lock;
int socks[MAX_CONNECTIONS];
char sin_addrs[MAX_CONNECTIONS][MAX_HOST_LEN];
int sin_ports[MAX_CONNECTIONS];
struct table *head;
struct config_params params;

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
int handle_command(int sock, char *cmd, struct config_params *params, struct table* head)
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

	// Lock thread
	pthread_mutex_lock (&lock);
	sscanf(cmd,"%s",cmdidentify);
	char buff[50];
	sprintf(buff,"Processing command '%s'\n", cmd);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);

	if (strcmp (cmdidentify, "AUTH") == 0){
		sscanf(cmd,"%*s %s %s",cmdusername,cmdpassword);
		if (strcmp (cmdusername,params->username)!=0 || strcmp (cmdpassword,params->password)!=0){
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
		int transac_id = atoi(arg);
		arg = strtok (NULL, " ");
		strcpy (cmdtable, arg);
		arg = strtok(NULL, " ");
		strcpy (cmdkey, arg);
		//Place the rest into cmdvalue.
		arg = strtok(NULL, " ");
		if (strcmp(arg,"NULL")!=0)
			arg[strlen(arg)] = ' ';
		strcpy (cmdvalue, arg);

		char datapath[MAX_PATH_LEN];
		strcpy (datapath, params->data_directory);
		strcat (datapath, cmdtable);
		success = setEntry(head, cmdtable, cmdkey, cmdvalue, datapath, params->policy, transac_id);
		//Error -1 = Table not found, -2 = Wrong column format/Invalid param, -3 = Key not found, -4 = Transaction aborted
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
		// Obsolete with implementation of on-disk storage
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
				char datapath[MAX_PATH_LEN];
				strcpy (datapath, params->data_directory);
				strcat (datapath, TABLE);
				success = setEntry(head, TABLE, KEY, buff2, datapath, params->policy, 0);


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

		// Get the time at the end of the experiment.
		gettimeofday(&end_time,NULL);
		double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

		//get difference in time
		double total_processing_time=t2-t1;
;
//		printf("The server processing time was %.6lf seconds.\n",total_processing_time);
		sprintf(buff,"The server processing time was %.6lf seconds.\n",total_processing_time);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
	}
	else if (strcmp (cmdidentify, "QUERY") == 0) {
		struct timeval start_time, end_time;
        	// Remember when the experiment started.
		gettimeofday(&start_time,NULL);
		double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

		//int numFilled = sscanf(cmd,"%*s %s %d %s", cmdtable, maxKeys, cmdvalue);
		//printf ("%d\n", numFilled);
		if (sscanf(cmd,"%*s %s %d %s", cmdtable, &maxKeys, cmdvalue) < 3) {
		//if (0) {
			arg = strtok (cmd, " ");
			arg = strtok (NULL, " ");
			strcpy (cmdtable, arg);
			arg = strtok (NULL, " ");
			maxKeys = atoi (arg);
			// Empty predicates.  Return all values from table.
			strcpy (cmdvalue, "LOAD_ALL");
		}
		else {
			arg = strtok (cmd, " ");
			arg = strtok (NULL, " ");
			strcpy (cmdtable, arg);
			arg = strtok (NULL, " ");
			maxKeys = atoi (arg);
			//Place the rest into cmdvalue.
			arg = strtok(NULL, " ");
			arg[strlen(arg)] = ' ';
			strcpy (cmdvalue, arg);
		}

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

//		printf("The server processing time was %.6lf seconds.\n",total_processing_time);
		sprintf(buff,"The server processing time was %.6lf seconds.\n",total_processing_time);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
	}

	// Unlock thread
	pthread_mutex_unlock(&lock);
	// For now, just send back the command to the client.
	strcat (cmd, "\n");
	sendall(sock, cmd, strlen(cmd));
	//sendall(sock, "\n", 1);

	return success;
}

// Function for handling commands from a single client.  To be used for multi-threading.
void* handle_client(void* ptr) {
	int num = *((int* )ptr);
	// Get commands from client.
	int wait_for_commands = 1;
	do {
		// Read a line from the client.
		char cmd[MAX_CMD_LEN];
		int status = recvline(socks[num], cmd, MAX_CMD_LEN);
		if (status != 0) {
			// Either an error occurred or the client closed the connection.
			wait_for_commands = 0;
		} else {
			if (!(cmd == NULL || strlen(cmd) < 2)) {
				// Handle the command from the client.
				int status = handle_command(socks[num], cmd, &params, head);
			}
			if (status != 0)
				wait_for_commands = 0; // Oops.  An error occured.
		}
	} while (wait_for_commands);

	// Close the connection with the client.
	close(socks[num]);
	socks[num] = -1;
	char buff[50];
	sprintf(buff,"Closed connection from %s:%d.\n", sin_addrs[num], sin_ports[num]);
	if (LOGGING == 1) logger(stdout, buff);
	else if (LOGGING == 2) logger(file, buff);
	return;
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
	pthread_mutex_init (&lock, NULL);
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
	//struct config_params params;
	//initialise the values for flags in params
	params.server_host_exist=0;
	params.server_port_exist=0;
	params.username_exist=0;
	params.password_exist=0;
	params.storage_policy_exist = 0;
	params.data_directory_exist = 0;
	params.concurrency_exist = 0;
	params.tableIndex=0;
	params.policy = 0;
	strcpy (params.data_directory, "");
	params.concurrency = -1;
	strcpy(params.table_name[0],"");
	int status = read_config(config_file, &params);
	if (status != 0 || strcmp(params.table_name[0],"") == 0 || params.concurrency == -1 || params.concurrency_exist == 0) {
		sprintf(buff,"Error processing config file.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}
	// If storage policy is set to on-disk then load all files from directory.
	FILE* diskTables[MAX_TABLES];
	char datapath[MAX_PATH_LEN];
	if (params.policy == 1) {
		if (params.data_directory == NULL || strcmp (params.data_directory, "") == 0) {
			sprintf(buff,"Error processing config file.\n");
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			exit(EXIT_FAILURE);
		}
		if (params.data_directory[strlen(params.data_directory) - 1] != '/')
			strcat (params.data_directory, "/");
		strcpy (datapath, params.data_directory);
		mode_t process_mask = umask(0);
		if (_mkdir(datapath) == -1) {
			sprintf(buff,"Data directory %s failed to be created.\n", datapath);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			exit(EXIT_FAILURE);
		}
		else {
			sprintf(buff,"Creating directory %s.\n", datapath);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
		}
		umask(process_mask);
	}
	// Initialize the tables in database
	//struct table *head;
	struct table *curr;
	int i, j;
	
	char TABLE[MAX_TABLE_LEN];
	char KEY[MAX_KEY_LEN];
	char TEMP[100];
	char values[MAX_COLUMNS_PER_TABLE][MAX_VALUE_LEN];
	char value[MAX_VALUE_LEN];
	char* buff2;
	int success = 0;

	if (params.tableIndex > MAX_TABLES) {
		sprintf(buff,"Error processing config file.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}
	head=malloc(sizeof(struct table));
	// Needs proper implementation from config parsing.
	if (params.numCol[0] > MAX_COLUMNS_PER_TABLE) {
		sprintf(buff,"Error processing config file.\n");
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		exit(EXIT_FAILURE);
	}
	head->numCol = params.numCol[0];
	head-> headIndex = -1;
	head->numEntries = 0;
	head->next=NULL;
	strcpy(head->name,params.table_name[0]);
	for (i = 0; i < head->numCol; i++) {
		strcpy(head->col[i], params.col[0][i]);
		if (params.type[0][i] > MAX_STRTYPE_SIZE) {
			sprintf(buff,"Error processing config file.\n");
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			exit(EXIT_FAILURE);
		}
		head->type[i] = params.type[0][i];
	}
	for (j = 0; j < MAX_RECORDS_PER_TABLE;j++) {
		head->entries[j] = malloc(sizeof(struct hashEntry));
		head->entries[j]->transac_count = 1;
		head->entries[j]->next = NULL;
		head->entries[j]->prev = NULL;
		head->entries[j]->deleted = -1;
	}
	// Load table for head.
	if (params.policy == 1) {
		// Open/Create file.
		strcat (datapath, head->name);
		diskTables[0] = fopen (datapath, "a+");
		if (diskTables[0] == NULL) {
			sprintf(buff,"Error opening file: %s.\n", datapath);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			exit(EXIT_FAILURE);
		}
		sprintf(buff,"Created/Opened file %s.\n", datapath);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		// Reopen file to start reading input from inside.
		freopen (datapath, "r", diskTables[0]);
		strcpy (TABLE, head->name);
		while(fscanf(diskTables[0],"%s",&TEMP)!=EOF) {
			if (strlen(TEMP)>19) {
				memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
				KEY[MAX_KEY_LEN - 1] = '\0';
			}
			else {
				strcpy(KEY,TEMP);
			}

			strcpy (value, "");
			strcat (value, head->col[0]);
			strcat (value, " ");
			fscanf(diskTables[0],"\t%s",&values[0]);
			strcat (value, trim(values[0]));
			for (j = 1; j < head->numCol - 1; j++) {
				strcat (value, ", ");
				strcat (value, head->col[j]);
				strcat (value, " ");
				fscanf(diskTables[0],"\t%s",&values[j]);
				strcat (value, trim(values[j]));
			}
			strcat (value, ", ");
			strcat (value, head->col[head->numCol - 1]);
			strcat (value, " ");
			fscanf(diskTables[0],"\t%s\n",&values[head->numCol - 1]);
			strcat (value, trim(values[head->numCol - 1]));

			success = setEntry(head, TABLE, KEY, value, NULL, 0, 0);
		}
		// Close File.
		fclose (diskTables[0]);
		if (LOGGING != 0)
			sprintf(buff, "File %s loaded and closed.\n",datapath);
		if (LOGGING == 1) logger(stdout, buff);
		else if (LOGGING == 2) logger(file, buff);
		strcpy (datapath, params.data_directory);
	}
	curr=head;
	for (i=1;i<params.tableIndex;i++){
		curr->next=malloc(sizeof(struct table));
		curr=curr->next;
		curr-> headIndex = -1;
		curr->numEntries = 0;
		strcpy(curr->name,params.table_name[i]);
		if (params.numCol[i] > MAX_COLUMNS_PER_TABLE) {
			sprintf(buff,"Error processing config file.\n");
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			exit(EXIT_FAILURE);
		}
		curr->numCol = params.numCol[i];
		for (j = 0; j < curr->numCol; j++) {
			strcpy(curr->col[j], params.col[i][j]);
			if (params.type[i][j] > MAX_STRTYPE_SIZE) {
				sprintf(buff,"Error processing config file.\n");
				if (LOGGING == 1) logger(stdout, buff);
				else if (LOGGING == 2) logger(file, buff);
				exit(EXIT_FAILURE);
			}
			curr->type[j] = params.type[i][j];
		}
		for (j = 0; j < MAX_RECORDS_PER_TABLE;j++) {
			curr->entries[j] = malloc(sizeof(struct hashEntry));
			curr->entries[j]->transac_count = 1;
			curr->entries[j]->next = NULL;
			curr->entries[j]->prev = NULL;
			curr->entries[j]->deleted = -1;
		}
		// Load files for all tables.
		if (params.policy == 1) {
			// Open/Create Files.
			strcat (datapath, curr->name);
			diskTables[i] = fopen (datapath, "a+");
			if (diskTables[i] == NULL) {
				sprintf(buff,"Error opening file: %s.\n", datapath);
				if (LOGGING == 1) logger(stdout, buff);
				else if (LOGGING == 2) logger(file, buff);
				exit(EXIT_FAILURE);
			}
			sprintf(buff,"Created/Opened file %s.\n", datapath);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			// Reopen Files to start reading data from inside.
			freopen (datapath, "r", diskTables[i]);
			strcpy (TABLE, curr->name);
			while(fscanf(diskTables[i],"%s",&TEMP)!=EOF) {
				if (strlen(TEMP)>19) {
					memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
					KEY[MAX_KEY_LEN - 1] = '\0';
				}
				else {
					strcpy(KEY,TEMP);
				}

				strcpy (value, "");
				strcat (value, curr->col[0]);
				strcat (value, " ");
				fscanf(diskTables[i],"\t%s",&values[0]);
				strcat (value, trim(values[0]));
				for (j = 1; j < curr->numCol - 1; j++) {
					strcat (value, ", ");
					strcat (value, curr->col[j]);
					strcat (value, " ");
					fscanf(diskTables[i],"\t%s",&values[j]);
					strcat (value, trim(values[j]));
				}
				strcat (value, ", ");
				strcat (value, curr->col[curr->numCol - 1]);
				strcat (value, " ");
				fscanf(diskTables[i],"\t%s\n",&values[curr->numCol - 1]);
				strcat (value, trim(values[curr->numCol - 1]));

				success = setEntry(curr, TABLE, KEY, value, NULL, 0, 0);
			}
			// Close Files.
			fclose (diskTables[i]);
			if (LOGGING != 0)
				sprintf(buff, "File %s loaded and closed.\n",datapath);
			if (LOGGING == 1) logger(stdout, buff);
			else if (LOGGING == 2) logger(file, buff);
			strcpy (datapath, params.data_directory);
		}

	}
	//make the last node point to NULL
	curr->next=NULL;
	//move curr to head again
	curr=head;

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
	pthread_t threads[MAX_CONNECTIONS+1];
	pthread_mutex_init (&lock, NULL);
	for (i = 0; i < MAX_CONNECTIONS; i++) {
		socks[i] = -1;
		sin_ports[i] = 0;
	}
	int num = 0;
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
	
		// Create threads if necessary.
		if (params.concurrency == 0) {
			socks[0] = clientsock;
			int temp = 0;
			strcpy (sin_addrs[0], inet_ntoa(clientaddr.sin_addr));
			sin_ports[0] = clientaddr.sin_port;
			handle_client((void*)&temp);
		}
		else if (params.concurrency == 1) {
			// Look for an available thread.
			for (i = 0; i < MAX_CONNECTIONS; i++) {
				if (socks[i] == -1) {
					num = i;
					socks[num] = clientsock;
					strcpy (sin_addrs[num], inet_ntoa(clientaddr.sin_addr));
					sin_ports[num] = clientaddr.sin_port;
					if (pthread_create (&threads[num], NULL, handle_client, (void*)&num) == 0) {
						sprintf(buff,"Thread %d created successfully.\n", i);
						if (LOGGING == 1) logger(stdout, buff);
						else if (LOGGING == 2) logger(file, buff);
						i = MAX_CONNECTIONS + 1;
					}
					else {
						sprintf(buff,"Thread failed to create successfully.\n");
						if (LOGGING == 1) logger(stdout, buff);
						else if (LOGGING == 2) logger(file, buff);
					}
				}
			}
			if (i == MAX_CONNECTIONS) {
				sprintf(buff,"No threads currently available.\n");
				if (LOGGING == 1) logger(stdout, buff);
				else if (LOGGING == 2) logger(file, buff);
			}
		}

	}

	// Stop listening for connections.
	close(listensock);
	fclose(file);
	
	// Free memory
	freeTable (head);

	return EXIT_SUCCESS;
}

