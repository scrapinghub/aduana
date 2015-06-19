#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include "domain_temp.h"

DomainTemp *
domain_temp_new(size_t length, float window) {
     DomainTemp *dh = calloc(1, sizeof(*dh));
     if (!dh)
          return 0;
     dh->table = calloc(length, sizeof(*dh->table));
     if (!dh->table) {
          free(dh);
          return 0;
     }
     dh->length = length;
     dh->window = window;

     return dh;
}

void
domain_temp_update(DomainTemp *dh, float t) {
     float k = 1.0 - (t - dh->time)/dh->window;
     if (k < 0)
          k = 0.0;

     for (size_t i=0; i<dh->length; ++i)
          dh->table[i].temp *= k;
     dh->time = t;
}

void
domain_temp_heat(DomainTemp *dh, uint32_t hash) {
     float temp_min = dh->table[0].temp;
     size_t i_min = 0;
     int found = 0;
     for (size_t i=0; i<dh->length && !found; ++i)
          if (dh->table[i].hash == hash) {
               dh->table[i].temp += 1.0;
               found = 1;
          } else if (dh->table[i].temp < temp_min){
               temp_min = dh->table[i].temp;
               i_min = i;
          }
     if (!found && temp_min < 1.0) {
          dh->table[i_min].hash = hash;
          dh->table[i_min].temp = 1.0;
     }
}

float
domain_temp_get(DomainTemp *dh, uint32_t hash) {
     for (size_t i=0; i<dh->length; ++i)
          if (dh->table[i].hash == hash) {
               return dh->table[i].temp;
          }
     return 0.0;
}

void
domain_temp_delete(DomainTemp *dh) {
     if (dh) {
          free(dh->table);
          free(dh);
     }
}

#if (defined TEST) && TEST
#include "test_domain_temp.c"
#endif // TEST
