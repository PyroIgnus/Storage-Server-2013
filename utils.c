/**
 * @file
 * @brief This file implements various utility functions that are
 * can be used by the storage server and client library. 
 */

#define _XOPEN_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include "utils.h"
#include <time.h>
/**
 * @brief Parse and process a line across the network.
 */
int sendall(const int sock, const char *buf, const size_t len)
{
	size_t tosend = len;
	while (tosend > 0) {
		ssize_t bytes = send(sock, buf, tosend, 0);
		if (bytes <= 0) 
			break; // send() was not successful, so stop.
		tosend -= (size_t) bytes;
		buf += bytes;
	};

	return tosend == 0 ? 0 : -1;
}

/**
 * In order to avoid reading more than a line from the stream,
 * this function only reads one byte at a time.  This is very
 * inefficient, and you are free to optimize it or implement your
 * own function.
 */
int recvline(const int sock, char *buf, const size_t buflen)
{
	int status = 0; // Return status.
	size_t bufleft = buflen;

	while (bufleft > 1) {
		// Read one byte from scoket.
		ssize_t bytes = recv(sock, buf, 1, 0);
		if (bytes <= 0) {
			// recv() was not successful, so stop.
			status = -1;
			break;
		} else if (*buf == '\n') {
			// Found end of line, so stop.
			*buf = 0; // Replace end of line with a null terminator.
			status = 0;
			break;
		} else {
			// Keep going.
			bufleft -= 1;
			buf += 1;
		}
	}
	*buf = 0; // add null terminator in case it's not already there.
	return status;
}
/**
 * @brief Ahelper funtion used to check if a table exists or not
 * @return If successful, return 1 if found, 0 if not found
 */
int table_exist(struct config_params *params,char *value){
	int i;
//	i=params->tableIndex;
	for (i=0;i< params->tableIndex;i++){
		if (strcmp(value,params->table_name[i])==0)
			return 1;
	}
	return 0;
}

/**
 * @brief Parse and process a line in the config file.
 */
int process_config_line(char *line, struct config_params *params)
{
	// Ignore comments.
	if (line[0] == CONFIG_COMMENT_CHAR)
		return 0;

	// Extract config parameter name and value.
	char name[MAX_CONFIG_LINE_LEN];
	char value[MAX_CONFIG_LINE_LEN];
	int items = sscanf(line, "%s %s\n", name, value);

	// Line wasn't as expected.
	if (items != 2)
		return -1;

	// Process this line.
	if (strcmp(name, "server_host") == 0) {
		if (params->server_host_exist==0){
			strncpy(params->server_host, value, sizeof params->server_host);
			params->server_host_exist=1;
		} else {
			errno_test=ERR_INVALID_PARAM;
			return -1;
		}
	} else if (strcmp(name, "server_port") == 0) {
		if (params->server_port_exist==0){
			params->server_port = atoi(value);
			params->server_port_exist=1;
		}	else{
			errno_test=ERR_INVALID_PARAM;
			return -1;
		}
	} else if (strcmp(name, "username") == 0) {
		if (params->username_exist==0){
			strncpy(params->username, value, sizeof params->username);
			params->username_exist=1;
		} else{
			errno_test=ERR_INVALID_PARAM;
			return -1;
		}
	} else if (strcmp(name, "password") == 0) {
		if (params->password_exist==0){
			strncpy(params->password, value, sizeof params->password);
			params->password_exist=1;
		} else {
			errno_test=ERR_INVALID_PARAM;
			return -1;
		}
	} else if (strcmp(name, "table") == 0) {
		// This loads all tables first.  Server will then check for duplicates afterwards
		if (table_exist(params,value)==0){
			strncpy(params->table_name[params->tableIndex], value, sizeof params->table_name[params->tableIndex]);
			params->tableIndex=params->tableIndex + 1;
		}
	}


	// else if (strcmp(name, "data_directory") == 0) {
	//	strncpy(params->data_directory, value, sizeof params->data_directory);
	//} 
	else {
		// Ignore unknown config parameters.
	}

	return 0;
}

/**
 * @brief Reads the config file to compare the values later on
 * @return If successful, return 0 if found, -1 if not found
 */
int read_config(const char *config_file, struct config_params *params)
{
	int error_occurred = 0;

	// Open file for reading.
	FILE *file = fopen(config_file, "r");
	if (file == NULL)
		error_occurred = 1;

	// Process the config file.
	while (!error_occurred && !feof(file)) {
		// Read a line from the file.
		char line[MAX_CONFIG_LINE_LEN];
		char *l = fgets(line, sizeof line, file);

		// Process the line.
		if (l == line)
			process_config_line(line, params);
		else if (!feof(file))
			error_occurred = 1;
	}

	return error_occurred ? -1 : 0;
}

/**
 * @brief helper funtion used to print
 * @return void
 */
void logger(FILE *file, char *message)
{
	fprintf(file,"%s",message);
	fflush(file);
}
/**
 * @brief Encrypts the password
 * @return char*, i.e. the encrypted password
 */
char *generate_encrypted_password(const char *passwd, const char *salt)
{
	if(salt != NULL)
		return crypt(passwd, salt);
	else
		return crypt(passwd, DEFAULT_CRYPT_SALT);
}

struct timeval * gettimeofday(struct timeval *currtime){

	time_t rawtime;

	time(&rawtime);
	currtime = localtime(&rawtime);

	return (currtime);
}

void printdifftime(struct timeval *start_time, struct timeval *end_time){

//	  double seconds;
//
//	  seconds = difftime(mktime(&start_time),mktime(&end_time));
//
//	  printf ("The time difference of the end-to-end execution is %.f \n", seconds);

}

double getdifftime(struct timeval *start_time, struct timeval *end_time){

	  double seconds;

	  seconds = difftime(mktime(&start_time),mktime(&end_time));

	  return seconds;

}


//int hash (char* key) {
//	int index = atoi (key);
//	return index;
//}
/**
 * @brief converts key into index; helper for hash table
 */
int hash(char *str){
	unsigned long hash = 5381;
    int c;
    while (c = *str++)
    	hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    hash=hash%MAX_RECORDS_PER_TABLE;



    return hash;
}
/**
 * @brief Helper funtion, used to probe through table to avoid collissions
 * @return If successful, return index , -1 if not successful
 */
int probeIndex (int index, int origIndex) {
	//This is linear probing
	int newIndex = index + 1;
	//Return to beginning of table
	if (newIndex > MAX_RECORDS_PER_TABLE)
		newIndex = 0;
	//Return -1
	else if (newIndex == origIndex) {
		return -1;
	}
	return newIndex;
}
/**
 * @brief gets the entry from hash table
 * @return If successful, return 1 if found, 0 if not found
 */
int getEntry (struct table* root, char* tableName, char* key) {
	struct table* node = root;
	int index = hash (key);
	int pIndex = index;
	int numProbes = 0;
	struct hashEntry* entry;// = malloc(sizeof(struct hashEntry));
	while (node != NULL && pIndex != -1) {
		// Check for the right table
		if (strcmp (tableName, node->name) == 0) {
			pIndex = hash (key);
			while (pIndex != -1  && node->numEntries > numProbes) {
				entry = node->entries[pIndex];
				// If not deleted, then check for same name and return value
				if (entry->deleted != -1 && strcmp (entry->name, key) == 0) {
					return atoi (entry->value);
				}
				// If entry is deleted/unused or wrong entry, then probe next index
				//if (entry->deleted == -1) {
				else {
					pIndex = probeIndex (pIndex, index);
					if (strcmp (entry->name, key) != 0)
						numProbes += 1;
				}
			}
			//Entry not found
			return -2;
		}
		else
			node = node->next;
	}
	// If table does not exist, return -1
	return -1;
}
/**
 * @brief Sets the value of the specified entry.
 * @return Returns 0 for a successful set and -1 otherwise. If value is NULL, then the pair is to be deleted.
 */
int setEntry (struct table* head, char* tableName, char* key, char* value) {
	struct table* node = head;
	int index = hash (key);
	int pIndex = index;
	int numProbes = 0;
	struct hashEntry* entry;// = malloc(sizeof(struct hashEntry));
	while (node != NULL && pIndex != -1) {
		// Check for the right table
		if (strcmp (tableName, node->name) == 0) {
			pIndex = hash (key);
			// Loop to check for existing entry
			while (pIndex != -1  && node->numEntries > numProbes) {
				entry = node->entries[pIndex];
				// If entry exists, then check for same name and set value and return 0
				if (entry->deleted != -1 && strcmp (entry->name, key) == 0) {
					// Deletes entry
					if (strcmp (value, "NULL") == 0) {
						entry->deleted = -1;
						node->numEntries -= 1;
					}
					// Edits entry
					else {
						char valueTemp[MAX_VALUE_LEN];
						sscanf(value,"%s",valueTemp);
						strcpy (entry->value, value);
					}
					return 0;
				}
				// If entry is deleted/unused or wrong entry, then probe next index
				//if (entry->deleted == -1) {
				else {
					pIndex = probeIndex (pIndex, index);
					if (strcmp (entry->name, key) != 0)
						numProbes += 1;
				}

			}
			numProbes = 0;
			// Now since entry cannot be found, entry must be added.
			if (node->numEntries < MAX_RECORDS_PER_TABLE) {
				pIndex = hash (key);
				while (pIndex != -1 && numProbes < MAX_RECORDS_PER_TABLE - node->numEntries) {
					entry = node->entries[pIndex];
					// If entry is deleted/unused, then set name and set value and return 0
					if (entry->deleted == -1) {
						char keyTemp[MAX_KEY_LEN];
						sscanf(key,"%s",keyTemp);
						strcpy (entry->name, key);
						char valueTemp[MAX_VALUE_LEN];
						sscanf(value,"%s",valueTemp);
						strcpy (entry->value, value);
						entry->deleted = 0;
						node->numEntries += 1;
						return 0;
					}
					if (entry->deleted != -1) {
						pIndex = probeIndex (pIndex, index);
						numProbes += 1;
					}
				}

			}
			// Table is full.  Return -1
			else
				return -1;
		}
		else
			node = node->next;
	}
	// If table does not exist, return -1
	return -1;
}
