#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#include "data_stream.h"

#define BUFFER_LENGTH 2048
#define COMMA_BATCH   16

typedef struct stanza_str {
  char *head;
  int  length;
  int  *commas;
  int  comma_idx;
  int  max_commas;
} stanza_t;

void delete_stanza(stanza_t *s)
{
  if(s) {
    if(s->commas) {
      free(s->commas);
    }
    free(s);
  }
}

int read_stanza(ds_source_state_t *src, stanza_t *stanza)
{
  unsigned long available;
  int n, idx;
  char c;
  
  if(!src || !stanza) {
    return -1;
  }

  available = src->length - (src->current - src->buffer);
  if((available == 0) && src->eof) {
    stanza->head = NULL;
    stanza->length = 0;
    return 0;
  }
  
  stanza->head = src->current;
  stanza->comma_idx = 0;
  stanza->length = 0;
  idx = 0;
  while(1) {
    if((available < src->max_buffer / 8) && !src->eof) {
      n = ds_load_data(src);
      if(n < 0) {
        return -1;
      }
      stanza->head = src->current;
      available = src->length - idx;
    } else if((available == 0) && src->eof) {
      break;
    }
    
    c = *(src->current + idx);
    available--;
    if(c == '\r' || c == '\n') {
      *(src->current + idx) = 0;
      src->current = src->current + idx + 1;
      if(available) {
        while(isspace(*(src->current)) && available) {
          available--;
          src->current++;
        }
      }
      break;
    } else {
      if(c == ',') {
        if(stanza->comma_idx == stanza->max_commas) {
          int *ptr = (int *)realloc(stanza->commas, (stanza->max_commas + COMMA_BATCH) * sizeof(int));
          memset(ptr + stanza->max_commas, 0, sizeof(int) * COMMA_BATCH);
          stanza->commas = ptr;
          stanza->max_commas += COMMA_BATCH;
        }
        stanza->commas[stanza->comma_idx++] = idx;
      }
      idx++;
    }
  }  

  stanza->length = idx;
  return idx;
}

typedef struct gps_type_template_str {
  char *type;
  char **fields;
  char **format;
  int  expected_commas;
  int  template_len;
} gps_type_template_t;

char *gga_fields[] = { 
  "time_delta", "type", "time", "latitude", "latitude_ns",
  "longitude", "latitude_ns", "fix_quality", "satellites", 
  "horizontal_dilution", "altitude", "altitude_units", "geoid_height",
  "geoid_height_units", "delta_last_dgps", "dgps_station_id", "checksum", NULL
};

char *gga_field_format[] = {
  "%s", "gga", "%s", "%s", "\"%s\"", "%s", "\"%s\"", "%s", "%s",
  "%s", "%s", "\"%s\"", "%s", "\"%s\"", "%s", "%s", "\"*%s\"", NULL
};

char *rmc_fields[] = {
  "time_delta", "type", "time", "status", "latitude", "latitude_ns",
  "longitude", "longitude_ew", "speed", "track_angle", "date",
  "magnetic_variation", "magnetic_variation_dir", "mode", "checksum",
  NULL
};

char *rmc_field_format[] = {
  "%s", "rmc", "%s", "\"%s\"", "%s", "\"%s\"", "%s", "\"%s\"",
  "%s", "%s", "%s", "%s", "\"%s\"", "\"%s\"", "\"%s\"",
  NULL
};

char *vtg_fields[] = {
  "time_delta", "type", "true_track",  "true_track_fixed", 
  "magnetic_track", "magnetic_track_fixed",
  "ground_speed_knots", "ground_speed_knots_units",
  "ground_speed_kmh", "ground_speed_kmh_units",
  "mode", "checksum",
  NULL
};

char *vtg_field_format[] = {
  "%s", "vtg", "%s", "\"%s\"", "%s", "\"%s\"", "%s", "\"%s\"",
  "%s", "\"%s\"", "\"%s\"", "\"%s\"",
  NULL
};


gps_type_template_t *gps_templates = NULL;
gps_type_template_t *generate_gps_templates()
{
  gps_type_template_t *templates = (gps_type_template_t *)calloc(5, sizeof(gps_type_template_t));
  templates[0].type = "gga";
  templates[0].fields = gga_fields;
  templates[0].format = gga_field_format;
  templates[0].expected_commas = 15;
  templates[0].template_len = -1;
  templates[1].type = "rmc";
  templates[1].fields = rmc_fields;
  templates[1].format = rmc_field_format;
  templates[1].expected_commas = 13;
  templates[1].template_len = -1;
  templates[2].type = "vtg";
  templates[2].fields = vtg_fields;
  templates[2].format = vtg_field_format;
  templates[2].expected_commas = 10;
  templates[2].template_len = -1;
  templates[3].type = NULL;
  return templates;
}
 
int parse_gps_fields(stanza_t *stanza, int field_cnt, char **fields)
{
  int r = -1, idx;
  
  if(stanza && fields && (field_cnt > 0)) {
    if(stanza->comma_idx == field_cnt) {
      if(stanza->head[stanza->length - 3] == '*') {
        // parse main fields
        fields[0] = stanza->head;
        for(idx = 0; idx < stanza->comma_idx; idx++)  {
          stanza->head[stanza->commas[idx]] = 0;
          fields[idx + 1] = stanza->head + stanza->commas[idx] + 1;
        }
  
        // parse checksum
        fields[idx + 1] = stanza->head + stanza->length - 2;
        *(stanza->head + stanza->length - 3) = 0;
      
        r = 0;
      }
    }
  }
  
  return r;
}

int populate_gps_template(char **template_fields, char **template_field_format, char **fields, 
                          int field_cnt, char *buffer, int buffer_len)
{
  int result = -1;
  int buffer_idx = 0, field_idx = 0;
  int tidx, fidx;
  int field_len, value_len, need_comma;

  tidx = 0;
  memcpy(buffer, "{ ", 2);
  buffer_idx = 2;
  need_comma = 0;
  while(template_fields[tidx] != NULL) {
    if(strnstr(template_field_format[tidx], "%s", strlen(template_field_format[tidx]))) {
      if(fields[field_idx] && strlen(fields[field_idx]) > 0) {
        if(buffer_idx + strlen(template_field_format[tidx]) + 
                        strlen(template_fields[tidx]) + strlen(fields[field_idx]) +
                        6 > buffer_len) {
          return -1;
        }
        if(need_comma) {
          snprintf(buffer + buffer_idx, buffer_len - buffer_idx, ", \"%s\": ", template_fields[tidx]);
        } else {
          snprintf(buffer + buffer_idx, buffer_len - buffer_idx, "\"%s\": ", template_fields[tidx]);
        }
        buffer_idx += strlen(buffer + buffer_idx);
        snprintf(buffer + buffer_idx, buffer_len - buffer_idx, template_field_format[tidx], fields[field_idx]);
        buffer_idx += strlen(buffer + buffer_idx);
        need_comma = 1;
      }
      field_idx++;
    } else {
      if(buffer_idx + strlen(template_field_format[tidx]) + 
                      strlen(template_fields[tidx]) + 6 > buffer_len) {
        return -1;
      }
      if(need_comma) {
        snprintf(buffer + buffer_idx, buffer_len - buffer_idx, ", \"%s\": \"%s\"", template_fields[tidx], template_field_format[tidx]);
      } else {
        snprintf(buffer + buffer_idx, buffer_len - buffer_idx, "\"%s\": \"%s\"", template_fields[tidx], template_field_format[tidx]);
      }      
      buffer_idx += strlen(buffer + buffer_idx);
      need_comma = 1;
    }
    tidx++;
  }
  if(buffer_idx + 3 > buffer_len) {
    return -1;
  }
  snprintf(buffer + buffer_idx, 3, " }");
  buffer_idx += 2;
  *(buffer + buffer_idx) = 0;
  
  return 0;
}

char *parse_gps_stanza(stanza_t *stanza, int template_idx)
{
  char *result = NULL;
  int len;
  char **fields;
  char **template_fields;
  char **template_field_format;
  int field_cnt;
  
  if(template_idx >= 0) {
    template_fields = gps_templates[template_idx].fields;
    template_field_format = gps_templates[template_idx].format;
    field_cnt = gps_templates[template_idx].expected_commas;
    
    if(gps_templates[template_idx].template_len == -1) {
      // need to initialize length
      int i = 0;
      int tlen = 0;
      while(template_fields[i] != NULL) {
        // field name + quotes + colon + space + value quotes + comma + json needed
        tlen += strlen(template_fields[i]) + 2 + 2 + 2 + 1;
        i++;
      }
      gps_templates[template_idx].template_len = tlen + 32;
    }
    
    fields = (char **)calloc(field_cnt + 4, sizeof(char *));
    if(fields) {
      if(parse_gps_fields(stanza, field_cnt, fields) == 0) {
        len = stanza->length + gps_templates[template_idx].template_len + field_cnt * 2;
        fields[1] = fields[0];
        result = calloc(len, sizeof(char));
        if(populate_gps_template(template_fields, template_field_format, fields + 1, field_cnt + 1, result, len) != 0) {
          free(result);
          result = NULL;
        }
      }
      free(fields);
    }
  }
  
  return result;
}

char *parse_simple_pid_stanza(stanza_t *stanza)
{
  int len;
  char *result = NULL;
  char *pid_template = "{ time_delta: %s, pid: \"%s\", value: %s }";
  char *fields[3];
  fields[0] = stanza->head;
  fields[1] = stanza->head + stanza->commas[0] + 1;
  fields[2] = stanza->head + stanza->commas[1] + 1;
  *(stanza->head + stanza->commas[0]) = 0;
  *(stanza->head + stanza->commas[1]) = 0;
  
  if(stanza && (stanza->comma_idx == 2)) {
    len = strlen(pid_template) + stanza->length + 8;
    result = (char *)calloc(len, sizeof(char));
    snprintf(result, len, pid_template, fields[0], fields[1], fields[2]);
  }
  
  return result;
}

char *parse_accelerometer_stanza(stanza_t *stanza)
{
  int len, i;
  char *result = NULL;
  char *pid_template = "{ \"time_delta\": %s, \"pid\": \"%s\", \"x_accel\": %s, \"y_accel\": %s, \"z_accel\": %s }";
  char *fields[5];
  fields[0] = stanza->head;
  for(i = 1; i < 5; i++) {
    fields[i] = stanza->head + stanza->commas[i - 1] + 1;
    *(stanza->head + stanza->commas[i - 1]) = 0;
  }
  
  if(stanza && (stanza->comma_idx == 4)) {
    len = strlen(pid_template) + stanza->length + 8;
    result = (char *)calloc(len, sizeof(char));
    snprintf(result, len, pid_template, fields[0], fields[1], fields[2], fields[3], fields[4]);
  }
  
  return result;
}

int is_gps_stanza(stanza_t *stanza)
{
  int is_gps = 0;
  if(stanza->head[stanza->commas[1] + 1] && 
       (stanza->commas[1] + 7 <= stanza->length)) {
    if(strncmp("$GP", (stanza->head + stanza->commas[0] + 1), 3) == 0) {
      is_gps = 1;
    }
  }
  
  return is_gps;
}


char *parse_stanza(stanza_t *stanza)
{
  // is this a GPS stanza?
  char *result = NULL;
  int i;
  
  
  
  if(stanza->comma_idx > 2) {
    if(is_gps_stanza(stanza)) {
      i = 0;
      while(gps_templates[i].type != NULL) {
        if(strncasecmp(gps_templates[i].type, (stanza->head + stanza->commas[0] + 4), 3) == 0) {
          break;
        }
        i++;
      }
      
      if(gps_templates[i].type != NULL) {
        result = parse_gps_stanza(stanza, i);
      } else {
        stanza->head[stanza->commas[0] + 7] = 0;
        fprintf(stderr, "Error - Unknown GPS stanza type: %s\n", (stanza->head + stanza->commas[0] + 4));
      }
    } else if(stanza->comma_idx == 4) {
      // check to see if it is accelerator data
      if(strncasecmp(stanza->head + stanza->commas[0] + 1, "20,", 3) == 0) {
        result = parse_accelerometer_stanza(stanza);
      }
    }
  } else if(stanza->comma_idx == 2) {
    // treat as a simple pid value_len
    result = parse_simple_pid_stanza(stanza);
  }
  
  return result;
}

int main(int argc, char **argv)
{
  gps_templates = generate_gps_templates();
  
  stanza_t *stanza = calloc(1, sizeof(stanza_t));
  ds_source_state_t *src = ds_open_file(argv[1], BUFFER_LENGTH);
  while(read_stanza(src, stanza) > 0) {
    char *s = parse_stanza(stanza);
    if(s) {
      fprintf(stdout, "%s\n", s);
      free(s);
    }
  }
  ds_close_file(src);
  delete_stanza(stanza);
}