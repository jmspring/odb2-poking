#ifndef _DATA_STREAM_H_
#define _DATA_STREAM_H_

#include <stdio.h>

typedef struct ds_source_state_str {
  FILE *infile;
  char *buffer;
  char *current;
  long length;
  int  eof;
  int  max_buffer;
} ds_source_state_t;

ds_source_state_t *ds_open_file(char *filename, int max_buffer);
void ds_close_file(ds_source_state_t *src);
int ds_load_data(ds_source_state_t *src);

#endif /* _DATA_STREAM_H_ */