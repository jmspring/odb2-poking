#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "data_stream.h"

ds_source_state_t *ds_open_file(char *filename, int max_buffer)
{
  ds_source_state_t *src = (ds_source_state_t *)calloc(1, sizeof(ds_source_state_t));
  src->max_buffer = max_buffer;
  if(filename) {
    src->infile = fopen(filename, "r");
    if(src->infile == NULL) {
      free(src);
      src = NULL;
    }
  } else {
    src->infile = stdin;
  }

  return src;  
}

void ds_close_file(ds_source_state_t *src)
{
  if(src) {
    if(src->infile && src->infile != stdin) {
      fclose(src->infile);
    }
  
    if(src->buffer) {
      free(src->buffer);
    }
    
    free(src);
  }
}

int ds_load_data(ds_source_state_t *src) 
{
  long read_offset = 0;
  long n;
  
  if(!src || !src->infile) {
    return -1;
  }
  
  if(!src->buffer) {
    src->buffer = calloc(src->max_buffer, sizeof(char));
    src->current = src->buffer;
    src->eof = 0;
  }

  if(src->eof) {
    return 0;
  }
  
  // need to keep what is unused
  if(src->current > src->buffer) {
    src->length = src->length - (src->current - src->buffer);
    memcpy(src->buffer, src->current, src->length);
    src->current = src->buffer;
    read_offset = src->length;
  }
  
  n = fread(src->buffer + read_offset, sizeof(char), src->max_buffer - read_offset, src->infile);
  if(n < src->max_buffer - read_offset) {
    if(feof(src->infile)) {
      src->eof = 1;
    } else if(ferror(src->infile)) {
      n = -1;
    }
  }
  src->length = read_offset + n;
  
  return n;
}
