/**
 * @file
 * @brief This file declares various utility functions that are
 * can be used by the storage server and client library.
 */

#ifndef	UTILS_H
#define UTILS_H

#include <stdio.h>
#include "storage.h"
#include <time.h>
/**
 * @brief Any lines in the config file that start with this character 
 * are treated as comments.
 */
static const char CONFIG_COMMENT_CHAR = '#';

/**
 * @brief The max length in bytes of a command from the client to the server.
 */
#define MAX_CMD_LEN (1024 * 8)
/**
 * @brief for error checking.
 */
int errno_test;

/**
 * @brief A macro to log some information.
 *
 * Use it like this:  LOG(("Hello %s", "world\n"))
 *
 * Don't forget the double parentheses, or you'll get weird errors!
 */
#define LOG(x)  {printf x; fflush(stdout);}

/**
 * @brief A macro to output debug information.
 * 
 * It is only enabled in debug builds.
 */
#ifdef NDEBUG
#define DBG(x)  {}
#else
#define DBG(x)  {printf x; fflush(stdout);}
#endif
/**
 * @brief Structs for the hash table implementation.
 *
 * It is only enabled in debug builds.
 */
// Structs for the hash table implementation (also acts as a circular linked list for all existing entries in a table.
struct hashEntry {
	/// Key(name)
	char key[MAX_KEY_LEN];

	/// Value
	char value[MAX_COLUMNS_PER_TABLE][MAX_VALUE_LEN];

	/// If equal to -1, then entry is deleted/unused.  Otherwise it exists.
	int deleted;

	// Next entry and previous entry
	struct hashEntry* next;
	struct hashEntry* prev;

};
/**
 * @brief Struct for including multiple tables
 *
 */
struct table {
	/// Table name
	char name[MAX_TABLE_LEN];

	// Column names
	char col[MAX_COLUMNS_PER_TABLE][MAX_COLNAME_LEN];

	// Number of current entries
	int numEntries;

	// Number of columns
	int numCol;

	/// Corresponding hash table
	struct hashEntry* entries[MAX_RECORDS_PER_TABLE];

	// Index of the "head" of the entry linked list.  If it is -1 then there is no current head.
	int headIndex;

	/// Next table
	struct table* next;

};

// Functions for hash table implementation
int hash (char* key);
int probeIndex (int index, int origIndex);
char* getEntry (struct table* root, char* tableName, char* key);	// Change return value to account for -1 entry values
int setEntry (struct table* root, char* tableName, char* key, char* value);
char* query (struct table* root, char* tableName, char* predicates, int maxKeys);

// Miscellaneous Helper Functions
int deleteEntry (struct hashEntry* entry, struct hashEntry* head);
int insertEntry (struct hashEntry* entry, struct hashEntry* head);
int checkPred (char* value, int op, char* opvalue);
void freeTable (struct table* node);

/**
 * @brief A struct to store config parameters.
 */
struct config_params {
	/// The hostname of the server.
	char server_host[MAX_HOST_LEN];

	/// The listening port of the server.
	int server_port;

	/// The storage server's username
	char username[MAX_USERNAME_LEN];

	/// The storage server's encrypted password
	char password[MAX_ENC_PASSWORD_LEN];

	///flag for server_host
	int server_host_exist;
	///flag for server_port
	int server_port_exist;
	///flag for username
	int username_exist;
	///flag for password
	int password_exist;

	///table name
	char table_name[MAX_TABLES][MAX_TABLE_LEN];
	///table index
	int tableIndex;

	/// The directory where tables are stored.
	//	char data_directory[MAX_PATH_LEN];
};

int table_exist(struct config_params *params,char *value);

/**
 * @brief Exit the program because a fatal error occured.
 *
 * @param msg The error message to print.
 * @param code The program exit return value.
 */
static inline void die(char *msg, int code)
{
	printf("%s\n", msg);
	exit(code);
}

/**
 * @brief Keep sending the contents of the buffer until complete.
 * @return Return 0 on success, -1 otherwise.
 *
 * The parameters mimic the send() function.
 */
int sendall(const int sock, const char *buf, const size_t len);

/**
 * @brief Receive an entire line from a socket.
 * @return Return 0 on success, -1 otherwise.
 */
int recvline(const int sock, char *buf, const size_t buflen);

/**
 * @brief Read and load configuration parameters.
 *
 * @param config_file The name of the configuration file.
 * @param params The structure where config parameters are loaded.
 * @return Return 0 on success, -1 otherwise.
 */
int read_config(const char *config_file, struct config_params *params);

/**
 * @brief Generates a log message.
 * 
 * @param file The output stream
 * @param message Message to log.
 */
void logger(FILE *file, char *message);

/**
 * @brief Default two character salt used for password encryption.
 */
#define DEFAULT_CRYPT_SALT "xx"

/**
 * @brief Generates an encrypted password string using salt CRYPT_SALT.
 * 
 * @param passwd Password before encryption.
 * @param salt Salt used to encrypt the password. If NULL default value
 * DEFAULT_CRYPT_SALT is used.
 * @return Returns encrypted password.
 */
char *generate_encrypted_password(const char *passwd, const char *salt);

/**
 * @brief Checks if the string is valid or not.
 *
 * @param str The String that is being passed
 * @param type The type of validation to be tested for.
 * @return Returns 0 if string is valid, 1 if invalid.
 */
int my_strvalidate(char *str, int type);
/**
 * @brief get the current time
 */
//struct timeval *gettimeofday(struct timeval *currtime);     //get the current time
/**
 * @brief print the difference of start and end time
 */
//void printdifftime(struct timeval *start_time, struct timeval *end_time);   // print the difference of start and end time
/**
 * @brief get the diff time
 */
//double getdifftime(struct timeval *start_time, struct timeval *end_time);     //get the diff time

#endif
