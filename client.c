/**
 * @file
 * @brief This file implements a "very" simple sample client.
 * 
 * The client connects to the server, running at SERVERHOST:SERVERPORT
 * and performs a number of storage_* operations. If there are errors,
 * the client exists.
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include "storage.h"
#include <time.h>
#include <sys/time.h>
#include "file.h"
#include "storage.c"
#include "utils.h"
#include "utils.c"

#define MAX_KEYS 10

//extern int errno;

/**
 * @brief Start a client to interact with the storage server.
 * @param SERVERHOST the host name of the server.
 * @param SERVERPORT the port number of the server.
 * @param SERVERUSERNAME the username the user uses to log in.
 * @param TABLE	the name of table is stored in TABLE.
 * @param KEY the key entered by user is stored in KEY.
 * @param value the value entered by the user is stored in value.
 *
 * If connect is successful, the client performs a storage_set/get() on
 * TABLE and KEY and outputs the results on stdout. Finally, it exists
 * after disconnecting from the server.
 *
 */
int main(int argc, char *argv[]) {

	struct timeval start_time, end_time;

	char SERVERHOST[MAX_HOST_LEN];
	int SERVERPORT;
	char SERVERUSERNAME[MAX_USERNAME_LEN];
	char SERVERPASSWORD[MAX_ENC_PASSWORD_LEN];
	char TABLE[MAX_TABLE_LEN];
	char KEY[MAX_KEY_LEN];
	char TEMP[100];
//	char TEMP[20];
	char value[MAX_VALUE_LEN];
	printf("> ---------------------\n"
			"> 1) Connect\n"
			"> 2) Authenticate\n"
			"> 3) Get\n"
			"> 4) Set\n"//
			"> 5) Disconnect\n"
			"> 6) Exit\n"
			"> 7) Read from file\n"
			"> 8) Query\n"
			"> Press '0' to display menu\n"
			"> ---------------------\n"
			">Please enter your selection: ");
	int inp;
	int status;
	//char keys[MAX_KEYS][MAX_KEY_LEN];
	//char** keys;
	//initKeys(&keys, MAX_KEYS, MAX_KEY_LEN);
	char* keys[MAX_KEYS];
	int i;
	for (i = 0; i < MAX_KEYS; i++)
		keys[i] = malloc(MAX_KEY_LEN);

	struct storage_record r;
	r.metadata[0] = 0;
	char buff [100];
	if (LOGGING==2){
		time_t rawtime;
		struct tm * timeinfo;
		time(&rawtime);
		timeinfo = localtime(&rawtime);
		strftime(buff, 80, "Client-%Y-%m-%d-%H-%M-%S.log", timeinfo);
		file = fopen(buff, "a");
	}
	scanf("%d", &inp);
	void *conn = NULL;
	while (inp != 6) {
		switch (inp) {
		case 1:
			// Connect to server
			printf("Hostname: ");
			scanf("%s", SERVERHOST);
			printf("Port: ");
			scanf("%d", &SERVERPORT);
			conn = storage_connect(SERVERHOST, SERVERPORT);
			if (!conn) {
				sprintf(buff, "Cannot connect to server @ %s:%d. Error code: %d.\n",
						SERVERHOST, SERVERPORT, errno);
				//				if (LOGGING == 1) logger(stdout, buff);
				//				else if (LOGGING == 2) logger(file, buff);
				printf(buff);
				break;
			}
			sprintf(buff, "Connect to server @ %s:%d.\n",
					SERVERHOST, SERVERPORT);
			//			if (LOGGING == 1) logger(stdout, buff);
			//			else if (LOGGING == 2) logger(file, buff);
			printf(buff);
			break;
		case 2:
			// Authenticate the client.
			printf("Username: ");
			scanf("%s", SERVERUSERNAME);
			printf("Password: ");
			scanf("%s", SERVERPASSWORD);
			status = storage_auth(SERVERUSERNAME, SERVERPASSWORD, conn);
			if (status != 0) {
				sprintf(buff, "storage_auth failed with username '%s' and password '%s'. "
						"Error code: %d.\n", SERVERUSERNAME, SERVERPASSWORD, errno);

				printf(buff);
				storage_disconnect(conn);
				break;
			}
			sprintf(buff, "storage_auth: successful.\n");
			printf(buff);
			break;
		case 4:
			// Issue storage_set
			printf("Value: ");
			scanf("%c", TEMP);
			fgets(value, MAX_VALUE_LEN, stdin);
//			if (strlen(TEMP)>MAX_VALUE_LEN - 1){
//				memcpy( value, &TEMP, MAX_VALUE_LEN - 1);
//				value[MAX_VALUE_LEN - 1] = '\0';
//			}
//			else {
//				strcpy(value,TEMP);
//			}
			printf("Key: ");
			scanf("%s", KEY);
//			if (strlen(TEMP)>MAX_KEY_LEN - 1){
//				memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
//				KEY[MAX_KEY_LEN - 1] = '\0';
//			}
//			else {
//				strcpy(KEY,TEMP);
//			}
			printf("Table: ");
			scanf("%s", TABLE);
			strncpy(r.value, value, sizeof r.value);
			status = storage_set(TABLE, KEY, &r, conn);
			if (status != 0) {
				sprintf(buff, "storage_set failed. Error code: %d.\n", errno);

				printf(buff);
				storage_disconnect(conn);
				break;
			}

			sprintf(buff, "storage_set: successful.\n");
			printf(buff);

			break;
		case 3:
			// Issue storage_get
			printf("Key: ");
			scanf("%s", TEMP);
			if (strlen(TEMP)>MAX_KEY_LEN){
				memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
				KEY[MAX_KEY_LEN - 1] = '\0';
			}
			else {
				strcpy(KEY,TEMP);
			}
			printf("Table: ");
			scanf("%s", TABLE);
			status = storage_get(TABLE, KEY, &r, conn);
			if (status != 0) {
				sprintf(buff, "storage_get failed. Error code: %d.\n", errno);
				printf(buff);
				storage_disconnect(conn);
				break;
			}
			sprintf(buff, "storage_get: the value returned for key '%s' is '%s'.\n",
					KEY, r.value);
			printf(buff);
			break;
		case 5:
			// Disconnect from server
			status = storage_disconnect(conn);
			if (status != 0) {
				sprintf(buff,"storage_disconnect failed. Error code: %d.\n", errno);
				printf(buff);
				break;
			}
			sprintf(buff, "storage_disconnect: successful.\n");
			printf(buff);
			break;
		case 7:
			printf ("Enter file name: ");
			scanf("%s", TABLE);
			sprintf(value, "FILESTORE");
			strncpy(r.value, value, sizeof r.value);
			sprintf(KEY, "EMPTY");
			status = storage_set(TABLE, KEY, &r, conn);
			if (status != 0) {
				sprintf(buff, "storage_set failed. Error code: %d.\n", errno);

				printf(buff);
				storage_disconnect(conn);
				break;
			}

			sprintf(buff, "storage_set: successful.\n");
			printf(buff);

			break;
		case 8:
			printf("Table: ");
			scanf("%s", TABLE);
			printf("Predicates (column name (operator) value, ...): ");
			// Need a function that can read a line of input.
			scanf ("%c", TEMP);
			fgets(value, MAX_VALUE_LEN, stdin);

			status = storage_query(TABLE, value, &keys, MAX_KEYS, conn);
			if (status < 0) {
				sprintf(buff, "storage_set failed. Error code: %d.\n", errno);

				printf(buff);
				storage_disconnect(conn);
				break;
			}
			int i = 0;
			printf ("There were %d entries that matched the predicates.\nThese include: ", status);
			for (i = 0; i < MAX_KEYS; i++) {
				sprintf (buff, "%s ", keys[i]);
				printf (buff);
				strcpy (keys[i], "");
			}
			sprintf(buff, "\nstorage_query: successful.\n");
			printf(buff);
			break;
//
//
//
		case 0:
//			print the command layout
			printf("> ---------------------\n"
					"> 1) Connect\n"
					"> 2) Authenticate\n"
					"> 3) Get\n"
					"> 4) Set\n"//
					"> 5) Disconnect\n"
					"> 6) Exit\n"
					"> 7) Read from file\n"
					"> 8) Query\n"
					"> Press '0' to display menu\n"
					"> ---------------------");
			break;
		default:
			printf(">invalid input. Please try again.\n");
			break;
		}
		printf("\n>Please enter your selection: ");
		scanf("%d", &inp);
	}
	sprintf(buff, "Exiting\n");
	printf(buff);
	return 0;
}

/*  Obsolete
// Issue storage_set
printf("Value and key would be taken from the file \n");

strcpy(TABLE,"census");

FILE *infile;
infile=fopen("census","r");

// Remember when the experiment started.
gettimeofday(&start_time,NULL);
double t1=start_time.tv_sec+(start_time.tv_usec/1000000.0);

while(fscanf(infile,"%s",&TEMP)!=EOF) {

	if (strlen(TEMP)>19){
		memcpy( KEY, &TEMP, MAX_KEY_LEN - 1);
		KEY[MAX_KEY_LEN - 1] = '\0';
	}
	else {
		strcpy(KEY,TEMP);
	}


fscanf(infile,"\t%s\n",&value);



strncpy(r.value, value, sizeof r.value);

status = storage_set(TABLE, KEY, &r, conn);
if (status != 0) {
	sprintf(buff, "storage_set failed. Error code: %d.\n", errno);
	//				if (LOGGING == 1) logger(stdout, buff);
	//				else if (LOGGING == 2) logger(file, buff);
	printf(buff);
	storage_disconnect(conn);
	break;
}

sprintf(buff, "stored for %s %s\n",KEY,value);
//			if (LOGGING == 1) logger(stdout, buff);
//			else if (LOGGING == 2) logger(file, buff);
printf(buff);

}

gettimeofday(&end_time,NULL);
double t2=end_time.tv_sec+(end_time.tv_usec/1000000.0);

//get difference in time
double total_processing_time=t2-t1;

sprintf(buff, "The total time is %.6lf seconds.\n",total_processing_time);
if (LOGGING == 1) logger(stdout, buff);
else if (LOGGING == 2) logger(file, buff);*/
