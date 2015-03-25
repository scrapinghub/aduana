#define _POSIX_C_SOURCE  200809L
#define _BSD_SOURCE

/* We need the following operations:

   + Translate URL
     For a given URL test if already in the database. If true, then
     return its index. If false assign new index. For example:

     www.google.com -> (in_db = true , index = 123  )
     www.yahoo.com  -> (in_db = false, index = 10009)

   + A new URL arrives, with a list of links
     1. Translate all URLS
     2. For all not already present links, add them to the database
     3. If the URL already in the database:
            Delete all existing edges starting at the URL
     4. Add all edges

   + Transverse the list of edges. Ideally it would be great if done in an
     arbitrary order.
*/

#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <string.h>

#include "hat-trie.h"
#include "lmdb.h"
#include "xxhash.h"

#define MAX_LINE_LENGTH 10000
#define HASH_URL 0

int
main(void) {
     char *line = (char*)malloc(MAX_LINE_LENGTH + 1);
     size_t len = 0;
     value_t idx = 0;
     clock_t delta;

     FILE *url = fopen("../test/data/index-00000", "r");
     FILE *test = fopen("../test/data/index-00000-rand", "r");

#if 1
     hattrie_t *ht = hattrie_create();     
     delta = clock();
     while (fgets(line, MAX_LINE_LENGTH, url)) {  
	  for (len = 0; 
	       line[len] != '\0' && !isblank(line[len]); 
	       len++);
	  if (len == 0)
	       continue;

	  if (len > MDB_MAXKEYSIZE) 
	       len = MDB_MAXKEYSIZE;

	  *hattrie_get(ht, line, len) = idx++;

	  if (idx % 10000000 == 0)
	       printf("%zu\n", idx);
     }
     delta = clock() - delta;
     printf("Average insert time: %.2e\n", (((float)delta)/CLOCKS_PER_SEC)/idx);
     fseek(url, 0, SEEK_SET);

     idx = 0;
     delta = clock();
     while (fgets(line, MAX_LINE_LENGTH, test)) {
	  hattrie_get(ht, line, strlen(line));
	  ++idx;
	  if (idx % 10000 == 0)
	       printf("%zu\n", idx);
     }
     delta = clock() - delta;
     printf("Average query time: %.2e\n", (((float)delta)/CLOCKS_PER_SEC)/idx);
     fseek(test, 0, SEEK_SET);

     hattrie_free(ht);
#endif

     int rc;
     MDB_env *env;
     MDB_dbi dbi;
     MDB_val key, data;
     MDB_txn *txn;
     MDB_cursor *cursor;

     rc = mdb_env_create(&env);
     if (rc) 
	  printf("create %d %s\n", rc, mdb_strerror(rc));
     rc = mdb_env_set_mapsize(env, (MDB_MAXKEYSIZE + sizeof(size_t))*60000000LL);
     if (rc)
	  printf("mapsize %d %s\n", rc, mdb_strerror(rc));
     rc = mdb_env_open(env, "./testdb", 0, 0664);
     if (rc)
	  printf("open %d %s\n", rc, mdb_strerror(rc));
#if 1
     rc = mdb_txn_begin(env, NULL, 0, &txn); 
     if (rc)
	  printf("begin %d %s\n", rc, mdb_strerror(rc)); 
     rc = mdb_dbi_open(txn, NULL, HASH_URL? MDB_INTEGERKEY: 0, &dbi);   
     if (rc)
	  printf("db open %d %s\n", rc, mdb_strerror(rc));
     rc = mdb_cursor_open(txn, dbi, &cursor);
     if (rc)
	  printf("cursor open %d %s\n", rc, mdb_strerror(rc));
     delta = clock();
     while (fgets(line, MAX_LINE_LENGTH+1, url)) {  
	  for (len = 0; 
	       line[len] != '\0' && !isspace(line[len]) && len < (MDB_MAXKEYSIZE - 1); 
	       len++);	  
	  line[len] = '\0';

#if HASH_URL
	  uint64_t hash = XXH64(line, len, 0);
	  key.mv_size = 8;
	  key.mv_data = &hash;
#else
	  key.mv_size = len + 1;
	  key.mv_data = line;
#endif
	  data.mv_size = sizeof(idx);
	  data.mv_data = &idx;
	  
	  rc = mdb_cursor_put(cursor, &key, &data, MDB_NOOVERWRITE);
	  if (rc) {
	       fprintf(stderr, "mdb_cursor_put: (%d) %s\n", rc, mdb_strerror(rc));
	  } else {
	       ++idx;
	       if (idx % 1000000 == 0)
		    printf("%zu\n", idx);
	  }
     }
     mdb_cursor_close(cursor);
     rc = mdb_txn_commit(txn);     
     if (rc) {
	  fprintf(stderr, "mdb_txn_commit: (%d) %s\n", rc, mdb_strerror(rc));
	  exit(EXIT_FAILURE);
     }
     rc = mdb_env_sync(env, 1);

     delta = clock() - delta;
     printf("Average insert time: %.2e\n", (((float)delta)/CLOCKS_PER_SEC)/idx);
     fseek(url, 0, SEEK_SET);
#endif
     rc = mdb_txn_begin(env, NULL, 0, &txn);
     rc = mdb_dbi_open(txn, NULL, HASH_URL? MDB_INTEGERKEY: 0, &dbi);   
     rc = mdb_cursor_open(txn, dbi, &cursor);
     printf("%zu %d %s\n", idx, rc, mdb_strerror(rc));
     idx = 0;
     delta = clock();
#if 1
     while (fgets(line, MAX_LINE_LENGTH+1, test)) {
	  for (len = 0; 
	       line[len] != '\0' && !isspace(line[len]) && len < (MDB_MAXKEYSIZE - 1); 
	       len++);	  
	  line[len] = '\0';

#if HASH_URL
	  uint64_t hash = XXH64(line, len, 0);
	  key.mv_size = 8;
	  key.mv_data = &hash;
#else
	  key.mv_size = len + 1;
	  key.mv_data = line;
#endif

	  if (mdb_cursor_get(cursor, &key, &data, MDB_SET) != 0)  {
	       fputs("Error retrieving key: ", stderr);
	       fputs(line, stderr);
	       fputs("\n", stderr);
	  } else {
	       ++idx;
	       if (idx % 1000000 == 0)
		    printf("%zu\n", idx);
	  }
     }     
#else
     while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
	  ++idx;
     }     
     printf("%zu %s\n", idx, mdb_strerror(rc));
#endif
     delta = clock() - delta;
     printf("Average query time: %.2e\n", (((float)delta)/CLOCKS_PER_SEC)/idx);
     fseek(test, 0, SEEK_SET);

     mdb_cursor_close(cursor);
     mdb_txn_abort(txn);
     mdb_env_close(env);

     fclose(url);
     fclose(test);

     return 0;
}
