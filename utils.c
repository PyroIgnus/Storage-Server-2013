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
#include <sys/stat.h>
#include <errno.h>

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
 * @brief A helper function used to check if a table exists or not
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
 * @brief Deletes the entry from the entry list.  If there is no current head then pass a NULL valued head.
 * @return Returns a pointer to the head if successful.  If the entry to be deleted is the head, then return a pointer to the next entry.  If head is the only entry, return NULL.
 */
// Deletes the entry from the entry list.  If there is no current head then pass a NULL valued head.
// Returns a pointer to the head if successful.  If the entry to be deleted is the head, then return a pointer to the next entry.  If head is the only entry, return NULL.
struct hashEntry* deleteEntry (struct hashEntry* entry, struct hashEntry* head) {
	if (head == NULL) {
		// Shouldn't have to go through here if code before handles this before hand.
		printf ("Weird error.\n");
		return NULL;
	}
	else if (head->prev == head && head->next == head) {
		// If this is the only entry in the linked list and needs to be deleted, head index needs to be changed to -1.
		return NULL;
	}
	else {
		// Deletes the entry.
		struct hashEntry* node = head;
		// If the head is the entry to be deleted, head index needs to be moved to the next entry.
		if (strcmp (entry->key, head->key) == 0) {
			struct hashEntry* next = head->next;
			head->prev->next = head->next;
			head->next->prev = head->prev;
			head->next = NULL;
			head->prev = NULL;
			return next;	
		}
		node = head->next;
		while (node != head) {
			if (strcmp (entry->key, node->key) == 0) {
				node->prev->next = node->next;
				node->next->prev = node->prev;
				node->next = NULL;
				node->prev = NULL;
				return head;
			}else
				node=node->next;

		}
		return head;
	}
//	return head;

}

/**
 * @brief Inserts the entry into the entry list.  If there is no current head then pass a NULL valued head.
 * @return Returns 0 if successful.  If there is no existing head then return 1.
 */
// Inserts the entry into the entry list.  If there is no current head then pass a NULL valued head.
// Returns 0 if successful.  If there is no existing head then return 1.
int insertEntry (struct hashEntry* entry, struct hashEntry* head) {
	if (head == NULL) {
		entry->next = entry;
		entry->prev = entry;
		return 1;
	}
	else {
		// Inserts the entry at the "end" of the circular linked list.
		head->prev->next = entry;
		entry->next = head;
		entry->prev = head->prev;
		head->prev = entry;
		return 0;
	}
	return 0;
}

/**
 * @brief Checks if a value meets the specified predicates.
 * @return Returns 0 if value meets the predicates, -1 if it does not, and -2 if the value exceeds the maximum string length.
 */
// Returns 0 if predicate is met and -1 otherwise
int checkPred (char* value, int op, char* opvalue, int type) {
	int isInt = my_strvalidate (opvalue, 2);
	int isChar = my_strvalidate (opvalue, 4);
	// Check the < operator
	if (op == -1 && type == -1 && isInt == 0) {
		if (atoi(value) < atoi (opvalue))
			return 0;
	}
	else if (op == -1) {
		return -2;	// Invalid data type.
	}
	// Check the == operator
	else if (op == 0) {
		if (strlen (value) > MAX_STRTYPE_SIZE - 1) {
			return -2;
		}
		if (strlen (value) > type - 1) {
			value[type - 1] = '\0';
		}
		if (strlen (value) > type || isChar == 1)
			return -2;
		if (strcmp (value, opvalue) == 0)
			return 0;
	}
	// Check the > operator
	else if (op == 1 && type == -1 && isInt == 0) {
		if (atoi(value) > atoi (opvalue)) {
			return 0;
		}
	}
	else if (op == 1) {
		return -2;	// Invalid data type.
	}
	return -1;
}

void initKeys (char*** A, int r, int c) {
	int i, j;
	*A = (char **)malloc(sizeof(char *)*r);
	for(i = 0; i< r; i++) {
		(*A)[i] = (char *)malloc(sizeof(char) *c);
		for(j = 0; j < c; j++) {
			(*A)[i][j] = "";
		}
	}
	return;
}

void freeTable (struct table* node) {
	if (node->next != NULL)
		freeTable (node->next);
	int i = 0;
	for (i = 0; i < MAX_RECORDS_PER_TABLE; i++) {
		node->entries[i]->next = NULL;
		node->entries[i]->prev = NULL;
		free (node->entries[i]);

	}
	free (node);
	return;
}

/**
 * @brief Parse and process a line in the config file.
 */
int process_config_line(char *line, struct config_params *params)
{
	// Ignore comments.
	if (line[0] == CONFIG_COMMENT_CHAR || strncmp(line,"\n",1)==0)
		return 0;

	// Extract config parameter name and value.
	char name[MAX_CONFIG_LINE_LEN];
	char value[MAX_CONFIG_LINE_LEN];
	int items = sscanf(line, "%s %s\n", name, value);

	// Line wasn't as expected.
	if (items != 2)
		return 1;

	// Process this line.
	if (strcmp(name, "server_host") == 0) {
		if (params->server_host_exist==0){
			strncpy(params->server_host, value, sizeof params->server_host);
			params->server_host_exist=1;
		} else {
			return 1;
		}
	} else if (strcmp(name, "server_port") == 0) {
		if (params->server_port_exist==0) {
			params->server_port = atoi(value);
			params->server_port_exist=1;
		} else {
			return 1;
		}
	} else if (strcmp(name, "username") == 0) {
		if (params->username_exist==0){
			strncpy(params->username, value, sizeof params->username);
			params->username_exist=1;
		} else{
			return 1;
		}
	} else if (strcmp(name, "password") == 0) {
		if (params->password_exist==0) {
			strncpy(params->password, value, sizeof params->password);
			params->password_exist=1;
		} else {
			return 1;
		}
	} else if (strcmp(name, "table") == 0) {
		// Parse value first into appropriate params variables.
		char *arg1;
		char *arg2;
		arg1 = strtok(line, " ");
		arg1 = strtok(NULL, " ");
		if (arg1 == NULL)
			return 1;
		char delims[]=":,[] \n";

		// This loads all tables first.  Server will then check for duplicates afterwards
		if (table_exist(params,arg1)==0){
			strncpy(params->table_name[params->tableIndex], arg1, sizeof params->table_name[params->tableIndex]);

			int count = 0;
			int isEmpty = 1;
			int i, j;
			char temp[100];
			int c;
			int val = 0;
			
			if ((int)arg1[0] == (int)',')
				return 1;
			arg1 = strtok( NULL, ",\n" );
			if (arg1 == NULL || strlen (trim(arg1)) < 3)
				return 1;
			while (arg1 != NULL) {
				j = strcspn (arg1, ":");
				if (j >= strlen (arg1) - 1) {
					return 1;
				}
				// Check for invalid spaces next to ":"
				c = (int)arg1[j - 1];
				if (c == (int)' ')
					return 1;
				c = (int)arg1[j + 1];
				if (c == (int)' ' || c == (int)'\n')
					return 1;
				strncpy (temp, arg1, j);
				temp[j] = '\0';
				strcpy(params->col[params->tableIndex][count],trim(temp));
				arg1 = arg1 + j + 1;
				// Check for column type
				j = strcspn (arg1, ",[\n\0");
				if (j > strlen (arg1)) {
					return 1;
				}
				c = (int)arg1[j];
				if (c == (int)'[') {
					if (j < 4)
						return 1;
					if ((int)arg1[j-4] != (int)'c' && (int)arg1[j-3] != (int)'h' && (int)arg1[j-2] != (int)'a' && (int)arg1[j-1] != (int)'r')
						return 1;
					arg1 = arg1 + j + 1;
					c = (int)arg1[0];
					if (c == (int)']' || c == (int)'\n' || c == 0)
						return 1;
					j = strcspn (arg1, "]\n");
					c = (int)arg1[j];
					if (c == (int)'\n' || c != (int)']')
						return 1;
					strncpy(temp, arg1, j);
					temp[j] = '\0';
					if (my_strvalidate(temp, 5) == 1)
						return 1;
					val = atoi(temp);
					params->type[params->tableIndex][count]=val;
				}
				else if (c == (int)',' || c == (int)'\n' || c == (int)'\0') {
					if (j < 3) {
						return 1;
					}
					if (strcmp (trim(arg1), "int") != 0) {	
						return 1;
					}
					params->type[params->tableIndex][count]=-1;
				}
				arg1 = arg1 + j;
				j = strcspn (arg1, ",");
				// Checks for line ending with comma and new line.
				if ((int)arg1[j+1] == (int)'\n')
					return 1;
				// Checks for line ending with comma and extra white space after.
				if (j > 1) {
					for (i = 1; i < j; i++) {
						if ((int)arg1[i] != (int)' ')
							return 1;
					}
					arg1 = arg1 + j + 1;
					j = strcspn (arg1, ",\n");
					for (i = 0; i < j; i++) {
						if ((int)arg1[i] != (int)' ')
							isEmpty = 0;
					}
					if (isEmpty == 1)
						return 1;
				}
				else
					arg1 = arg1 + 1;
				isEmpty = 1;
				count++;
				arg1 = strtok (NULL, "\n,");
			}

			params->numCol[params->tableIndex] = count;
			params->tableIndex=params->tableIndex + 1;
		}
		else
			return 1;
	}
	else if (strcmp(name, "storage_policy") == 0) {
		if (strlen (value) > 10 || params->storage_policy_exist == 1)
			return 1;
		if (strcmp (value, "in-memory") == 0) {
			params->policy = 0;
			params->storage_policy_exist = 1;
		}
		else if (strcmp (value, "on-disk") == 0) {
			params->policy = 1;
			params->storage_policy_exist = 1;
			if (params->data_directory_exist > 1)
				return 1;
		}
		else
			return 1;
	}
	else if (strcmp(name, "data_directory") == 0) {
		if (params->policy == 1 && params->data_directory_exist == 0) {
			strncpy(params->data_directory, value, sizeof params->data_directory);
			params->data_directory_exist += 1;
		}
		else if (params->policy == 0 && params->data_directory_exist == 1)
			return 1;
	}
	else if (strcmp(name, "concurrency") == 0) {
		if (params->concurrency_exist == 0){
			if (atoi(value) != 0 && atoi(value) != 1)
				return 1;
			params->concurrency = atoi(value);
			params->concurrency_exist=1;
		}
		else {
			return 1;
		}
	} 
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
			error_occurred = process_config_line(line, params);
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
	if (newIndex > MAX_RECORDS_PER_TABLE - 1)
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
char* getEntry (struct table* root, char* tableName, char* key) {
	if (strlen(key)>MAX_KEY_LEN - 1) {
		key[MAX_KEY_LEN - 1] = '\0';
	}
	if (strlen(tableName)>MAX_TABLE_LEN - 1) {
		tableName[MAX_TABLE_LEN - 1] = '\0';
	}
	struct table* node = root;
	char result[MAX_VALUE_LEN] = "";
	int i = 0;
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
				if (entry->deleted != -1 && strcmp (entry->key, key) == 0) {
					sprintf (result, "%d ", entry->transac_count);
					for (i = 0; i < node->numCol; i++) {
						strcat (result, node->col[i]);
						strcat (result, " ");
						strcat (result, entry->value[i]);
						if (i < node->numCol - 1)
							strcat (result, ", ");
					}
					//strcat (result, entry->value[0]);
					return result;
				}
				// If entry is deleted/unused or wrong entry, then probe next index
				else {
					pIndex = probeIndex (pIndex, index);
					if (strcmp (entry->key, key) != 0)
						numProbes += 1;
				}
			}
			//Entry not found
			return "-2";
		}
		else
			node = node->next;
	}
	// If table does not exist, return -1
	return "-1";
}
/**
 * @brief Sets the value of the specified entry.
 * @return Returns 0 for a successful set and -1 otherwise. If value is NULL, then the pair is to be deleted.
 */
int setEntry (struct table* head, char* tableName, char* key, char* value, char* path, int writeEn, int transac_id) {
	if (strlen(key)>MAX_KEY_LEN - 1) {
		key[MAX_KEY_LEN - 1] = '\0';
	}
	if (strlen(tableName)>MAX_TABLE_LEN - 1) {
		tableName[MAX_TABLE_LEN - 1] = '\0';
	}
	struct table* node = head;
	int index = hash (key);
	int pIndex = index;
	int numProbes = 0;
	int i = 0;
	int j = 0;
	char parsedValues[MAX_COLUMNS_PER_TABLE][MAX_STRTYPE_SIZE];
	char colNames[MAX_COLUMNS_PER_TABLE][MAX_COLNAME_LEN];
	int numCols = 0;;
	// Parse column names and values
	if (strcmp (value, "NULL") != 0) {
		if ((int)trim(value)[0] == (int)',')
			return -2;
		if ((int)trim(value)[strlen(trim(value)) - 1] == (int)',')
			return -2;
//		char delims[]=" ";
		char* arg;

		//strcpy(delims, " ");
		arg = strtok (value, " ");
		if (arg == NULL || my_strvalidate (trim(arg), 1)) {
			return -2;
		}
		while (arg != NULL) {
			if (my_strvalidate (trim(arg), 1)) {
				return -2;
			}
			strcpy (colNames[i], trim(arg));
//			strcpy(delims, ",");
			arg = strtok (NULL, ",\n");
			if (arg == NULL)
				return -2;
//			strcpy (arg, trim(arg));
			strcpy (parsedValues[i], trim(arg));
			i += 1;
			numCols += 1;
//			strcpy(delims, " ");
			arg = strtok (NULL, " ");
		}
	}

	struct hashEntry* entry;
	while (node != NULL) {;
		// Check for the right table
		if (strcmp (tableName, node->name) == 0) {
			//pthread_mutex_lock (&lock);
			// Check for the right column names in the right format and right data types.
			if (strcmp(value, "NULL")!=0){
				if (numCols != node->numCol)
					return -2;	// Wrong number of columns.
				for (i = 0; i < node->numCol; i++) {
					if (strcmp (node->col[i], colNames[i]) != 0)
						return -2;	// Wrong column name.
					if (node->type[i] == -1) {
						if (my_strvalidate (parsedValues[i], 2)) {
							return -2;	// Invalid data type.
						}
					}
					else {
						if (my_strvalidate (parsedValues[i], 4)) {
							return -2;	// Invalid data type.
						}
					}
				}
			}
			// If input is in the right format continue to search hash table.
			if (node->numEntries > 0)
				entry = node->entries[node->headIndex];
			// Loop to check for existing entry
			while (node->numEntries > numProbes) {
				// If entry exists, then check for same name and set value and return 0
				if (entry->deleted != -1 && strcmp (entry->key, key) == 0) {
					// Check to see if this is part of the same transaction.
					if (transac_id != 0 && transac_id != entry->transac_count)
						return -4;
					// Deletes entry
					if (strcmp (value, "NULL") == 0) {
						struct hashEntry* newHead = deleteEntry (entry, node->entries[node->headIndex]);
						if (newHead == NULL) {
							node->headIndex = -1;
						}
						else {
							node->headIndex = newHead->index;
						}
						entry->deleted = -1;
						node->numEntries -= 1;
					}
					// Edits entry
					else {
						// Store values.
						for (i = 0; i < node->numCol; i++) {
							if (node->type[i] != -1 && strlen(parsedValues[i])>node->type[i] - 1) {
								parsedValues[i][node->type[i] - 1] = '\0';
							}
							if (strcmp (entry->value[i], parsedValues[i]) != 0)
								strcpy (entry->value[i], parsedValues[i]);
						}
						entry->transac_count += 1;
					}
					if (writeEn) {
						char datapath[MAX_PATH_LEN];
						strcpy (datapath, path);
						FILE* tFile = fopen (datapath, "w");
						numProbes = 0;
						if (node->numEntries > 0)
							entry = node->entries[node->headIndex];
						while (node->numEntries > numProbes) {
							fprintf (tFile, "%s", entry->key);
							for (i = 0; i < node->numCol; i++) {
								fprintf (tFile, "\t%s", entry->value[i]);
							}
							fprintf (tFile, "\n");
							entry = entry->next;
							numProbes += 1;
						}
						fclose (tFile);
					}
					return 0;
				}
				// If entry is deleted/unused or wrong entry, then probe next index
				else {
					entry = entry->next;
					numProbes += 1;
				}

			}
			if (strcmp (value, "NULL") == 0) {
				return -3;	// Key not found.
			}
			numProbes = 0;
			// Now since entry cannot be found, entry must be added.
			if (node->numEntries <= MAX_RECORDS_PER_TABLE) {
				pIndex = hash (key);
				while (pIndex != -1 && numProbes <= MAX_RECORDS_PER_TABLE) {
					entry = node->entries[pIndex];
					// If entry is deleted/unused, then set name and set value and return 0
					if (entry->deleted == -1) {
						if (strcmp (entry->key, key) != 0) {
							entry->transac_count = 1;
							strcpy (entry->key, key);
						}
						// Store values
						for (i = 0; i < node->numCol; i++) {
							if (node->type[i] != -1 && strlen(parsedValues[i])>node->type[i] - 1) {
								parsedValues[i][node->type[i] - 1] = '\0';
							}
							strcpy (entry->value[i], parsedValues[i]);
						}
						entry->deleted = 0;
						entry->index = pIndex;
						int status;
						if (node->headIndex == -1) {
							status = insertEntry (entry, NULL);
							node->headIndex = pIndex;
						}
						else {
							status = insertEntry (entry, node->entries[node->headIndex]);
						}
						node->numEntries += 1;
						if (writeEn) {
							char datapath[MAX_PATH_LEN];
							strcpy (datapath, path);
							FILE* tFile = fopen (datapath, "a");
							fprintf (tFile, "%s", entry->key);
							for (i = 0; i < node->numCol; i++) {
								fprintf (tFile, "\t%s", entry->value[i]);
							}
							fprintf (tFile, "\n");
							fclose (tFile);
						}
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

/**
 * @brief Queries the specified table using the specified predicates.
 * @return Returns a string with the number of keys returned and their names.
 */
// Queries the specified table using the specified predicates and returns a string with the number of keys returned and their names.
char* query (struct table* root, char* tableName, char* predicates, int maxKeys) {
	if (strlen(tableName)>MAX_TABLE_LEN) {
		tableName[MAX_TABLE_LEN - 1] = '\0';
	}
	struct table* node = root;
	struct hashEntry* entry;
	char colNames[MAX_COLUMNS_PER_TABLE][MAX_COLNAME_LEN];
	int colIndices[MAX_COLUMNS_PER_TABLE];
	// Operators are <, =, > and are represented by -1, 0, 1 respectively.
	int operators[MAX_COLUMNS_PER_TABLE];
	char parsedValues[MAX_COLUMNS_PER_TABLE][MAX_STRTYPE_SIZE];
	int numCols = 0;
	int i, j;
	int numProbed = 0;
	int numKeys = 0;
	int predsMet = 0;
	int colMatch = 0;
	char result[MAX_VALUE_LEN];
	char buff[MAX_VALUE_LEN];

	// Parse predicates here into colNames, operators and parsedValues in the order that you find them.  operators should be represented as stated above.
	char* arg;
	char temp[MAX_COLNAME_LEN];
	int c;

	if ((int)trim(predicates)[0] == (int)',')
		return "-2";
	if ((int)trim(predicates)[strlen(trim(predicates)) - 1] == (int)',')
		return "-2";

	if (strcmp (predicates, "LOAD_ALL") == 0) {
		// Loop through all existing tables.
		while (node != NULL) {
			// Check for right table.
			if (strcmp (tableName, node->name) == 0) {
				// Proceed to query.
				if (node->numEntries > 0)
					entry = node->entries[node->headIndex];
				else
					return "0";	// If there are no entries then return 0;

				// Iterate through all the records in the linked list.
				while (node->numEntries > numProbed) {
					// Add it to the result.
					if (numKeys < maxKeys) {
						if (numKeys != 0) {
							strcat (result, " ");
							strcat (result, entry->key);
						}
						if (numKeys == 0)
							strcpy (result, entry->key);
					}
					numKeys += 1;
					// Add 1 to numProbed.
					numProbed += 1;
					entry = entry->next;
				}
				// Once all entries have been accounted for, return the key list and the number of keys found. (numKeys key1 key2 ...)
				sprintf (buff, "%d", numKeys);
				strcat (buff, " ");
				strcat (buff, result);
				return buff;
			}
			node = node->next;
		}
		// Table not found.
		return "-1";
	}
	else {
		arg = strtok (predicates, ",");
		if (arg == NULL) {
			return "-2";	// Shouldn't have to ever execute this.
		}
		i = 0;

		while (arg != NULL) {
			j = strcspn (arg, "<=>");
			if (j >= strlen (arg) - 2) {
				return "-2";	// If operator is not found, return "-2".
			}
			strncpy (temp, arg, j);
			temp[j] = '\0';
			// Set column name
			strcpy (colNames[i], trim(temp));	
			// Set operators
			c = (int)arg[j];
			if (c == (int)'<')
				operators[i] = -1;
			else if (c == (int)'=')
				operators[i] = 0;
			else if (c == (int)'>')
				operators[i] = 1;
			else {
				return "-2";	// Not an acceptable operator.
			}
			// Set values
			arg = arg + j + 1;
			strcpy (parsedValues[i], trim(arg));
			// Increment and set up for next set of predicates
			i += 1;
			numCols += 1;
			arg = strtok (NULL, ",");
		}

		// Loop through all existing tables.
		while (node != NULL) {
			// Check for right table.
			if (strcmp (tableName, node->name) == 0) {
				// If right table is found, check if given column names match.
				for (i = 0; i < numCols; i++) {
					for (j = 0; j < node->numCol; j++) {
						if (strcmp (colNames[i], node->col[j]) == 0) {
							colMatch = 1;
							colIndices[i] = j;	// Sets index
							j = node->numCol;	// Stops this loop to check next column name.
						}
					}
					if (colMatch == 0) {
						return "-2";	// Wrong column format.
					}
					colMatch = 0;
				}
				// Once all column names have been checked, proceed to query.
				if (node->numEntries > 0)
					entry = node->entries[node->headIndex];
				else
					return "0";	// If there are no entries then return 0;

				// Iterate through all the records in the linked list.
				while (node->numEntries > numProbed) {
					// Iterate through the specified predicate column names.
					for (j = 0; j < numCols; j++) {
						// If predicates are not met exit, this for loop.
						i = checkPred (entry->value[colIndices[j]], operators[j], parsedValues[j], node->type[colIndices[j]]);
						if (i == -2)
							return "-2";	// Invalid data type.
						if (i == -1) {
							predsMet = -1;
							// Stop checking this entry for valid predicates.
							j = numCols;
						}
					}
					// If all predicates are met then add it to the result.
					if (predsMet == 0) {
						if (numKeys < maxKeys) {
							if (numKeys != 0) {
								strcat (result, " ");
								strcat (result, entry->key);
							}
							if (numKeys == 0)
								strcpy (result, entry->key);
						}
						numKeys += 1;
					}
					// Reset predsMet for next entry and add 1 to numProbed.
					predsMet = 0;
					numProbed += 1;
					entry = entry->next;
				}
				// Once all entries have been accounted for, return the key list and the number of keys found. (numKeys key1 key2 ...)
				sprintf (buff, "%d", numKeys);
				strcat (buff, " ");
				strcat (buff, result);
				return buff;
			}
			node = node->next;
		}
		// Table not found.
		return "-1";
	}
}

/**
 * @brief Validates a string based on the type specified.
 * @return Returns 1 if it fails and 0 otherwise.
 */
int my_strvalidate(char *str, int type) {

	int i,j;
  	j=strlen(str);
  	int k;
	// Alphanumeric chars only.
	if (type == 1) {
   		for (i = 0; i < j; i++){
	  		k=(int)str[i];
      			if (!((k>=(int)'a'&&k<=(int)'z') || (k>=(int)'A'&&k<=(int)'Z') || (k>=(int)'0' && k<=(int)'9'))) {
    	  			return 1;
      			}
   		}
	}
	// Integers only.
	else if (type == 2) {
		for (i = 0; i < j; i++){
	  		k=(int)str[i];
      			if (!(k >= (int)'0' && k <= (int)'9')) {
      				if (i == 0 && k != 45 && k != 43)	// 45 = (int)'-', 43 = (int) '+'
      					return 1;
      			}
   		}
	}
	// Specific to hostname.
	else if (type == 3) {
		if (strcmp (str, "localhost") == 0) {
			return 0;
		}
		for (i = 0; i < j; i++){
	  		k=(int)str[i];
      			if (!((k>=(int)'a'&&k<=(int)'z') || (k>=(int)'A'&&k<=(int)'Z') || (k >= (int)'0' && k <= (int)'9') || (k == (int)'.') )) {
    	  			return 1;
      			}
   		}
	}
	// Alphanumeric with spaces.
	else if (type == 4) {
		for (i = 0; i < j; i++){
	  		k=(int)str[i];
      			if (!((k>=(int)'a'&&k<=(int)'z') || (k>=(int)'A'&&k<=(int)'Z') || (k>=(int)'0' && k<=(int)'9') || (k == (int)' '))) {
    	  			return 1;
      			}
   		}
	}
	// Numbers only.
	else if (type == 5) {
		for (i = 0; i < j; i++){
	  		k=(int)str[i];
      			if (!(k >= (int)'0' && k <= (int)'9')) {
      				return 1;
      			}
   		}
	}
  	return 0;
}

/**
 * @brief Removes whitespace from the front and the end.
 * @return Returns a string where any white space before and after are removed from the input string.
 */
// Returns a string where any white space before and after are removed from the input string.
char* trim (char* str) {
	char* result;
	if (str == NULL)
		return NULL;
	if (strcmp == "")
		return str;
	int len = strlen(str);
	if (len < 1)
		return str;
	int i = 0;
	for (i = 0; i < len; i++) {
		if (str[i] != ' ') {
			result = str+i;
			i = len;
		}
	}
	len = strlen(result);
	for (i = len - 1; i > -1; i--) {
		if (result[i] != ' ') {
			result[i + 1] = '\0';
			i = -1;
		}
	}
	return result;
}

// Taken from http://nion.modprobe.de/blog/archives/357-Recursive-directory-creation.html and edited.
int _mkdir(const char *dir) {
        char tmp[256];
        char *p = NULL;
        size_t len;
 	
        snprintf(tmp, sizeof(tmp),"%s",dir);
        len = strlen(tmp);
	if (tmp[0] != '.') {
		if(tmp[len - 1] == '/')
		        tmp[len - 1] = 0;
		for(p = tmp + 1; *p; p++)
		        if(*p == '/') {
		                *p = 0;
		                if (mkdir(tmp, S_IRWXO | S_IRWXU | S_IRWXG) == -1 && errno != EEXIST)
					return -1;
		                *p = '/';
		        }
		if (mkdir(tmp, S_IRWXO | S_IRWXU | S_IRWXG) == -1 && errno != EEXIST)
			return -1;
	}
	else {
		if (mkdir(tmp, S_IRWXO | S_IRWXU | S_IRWXG) == -1 && errno != EEXIST)
			return -1;
	}
	return 0;
}
