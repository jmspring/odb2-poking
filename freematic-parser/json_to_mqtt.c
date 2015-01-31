#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include <MQTTClient.h>
#include <MQTTClientPersistence.h>

#include "data_stream.h"

#define BUFFER_LENGTH 2048

typedef struct json_msg_str {
  char *body;
  int  length;
} json_msg_t;

void reset_json_msg(json_msg_t *msg) 
{
  if(msg) {
    if(msg->body) {
      free(msg->body);
    }
    msg->length = 0;
  }
}

void free_json_msg(json_msg_t *msg)
{
  if(msg) {
    reset_json_msg(msg);
    free(msg);
  }
}

int next_message(ds_source_state_t *src, char *delimiter, json_msg_t *msg)
{
  unsigned long available;
  int n, idx, dlen, more_data;
  char c;
  
  if(!src || !delimiter || !msg) {
    return -1;
  }

  dlen = strlen(delimiter);

  msg->body = NULL;
  msg->length = 0;
  available = src->length - (src->current - src->buffer);
  if((available == 0) && src->eof) {
    return 0;
  }

  idx = 0;
  more_data = 0;
  while(1) {
    if(more_data || ((available < src->max_buffer / 8) && !src->eof)) {
      n = ds_load_data(src);
      if(n < 0) {
        return -1;
      }
      available = src->length - idx;
    }
    if(available == 0) {
      idx = 0;
      break;
    }
    
    c = *(src->current + idx);
    if(c == delimiter[0]) {
      if(available >= dlen) {
        n = 1;
        while(n < dlen && *(src->current + idx + n) == delimiter[n]) {
          n++;
        }
        if(n == dlen) {
          msg->body = (char *)calloc(idx + 1, sizeof(char));
          msg->length = idx;
          memcpy(msg->body, src->current, idx);
          src->current = src->current + idx + dlen;
          break;
        } else {
          idx++;
          available--;
          continue;
        }
      } else {
        if(more_data) {
          idx = 0;
          break;
        } else {
          more_data = 1;
          continue;
        }
      }
    } else {
      idx++;
      available--;
    }
  }  

  return idx;
}

void usage(char *command_line)
{
	printf("mqtt publisher\n");
	printf("Usage: %s <options>, where options are:\n", command_line);
	printf("  -h <hostname> -- host (default: localhost)\n");
	printf("  -p <port> -- port (default: 1883)\n");
	printf("  -q <qos> -- qos (default: 0)\n");
	printf("  -r -- retained (default: off)\n");
	printf("  -d <delim> -- delimiter (default: \\n)");
	printf("  -c <clientid> -- clientid (default: hostname+timestamp)");
	printf("  -m <len> -- maximum data length (default: 2048)\n");
	printf("  -u <username> -- username (default: none)\n");
	printf("  -w <password> -- password (default: none)\n");
	printf("  -t <topic> -- topic to publish to (default: test)\n");
	exit(-1);
}

char *get_client_id(void)
{
  char buffer[512];
  int len;
  memset(buffer, 0, 512);
  gethostname(buffer, 256);
  len = strlen(buffer);
  snprintf(buffer + len, 512-len, "%u", (unsigned)time(NULL));
  return strdup(buffer);
}

struct config_str {
  char    *hostname;
  int     port;
  char    *username;
  char    *password;  
  char    *topic;
  int     qos;
  int     retained;
  char    *delimiter;
  char    *client_id;
  int     maximum_length;
  char    *input_file;
};

struct config_str *config_base(void)
{
  struct config_str *config = (struct config_str *)calloc(1, sizeof(struct config_str));
  if(config) {
    config->hostname = strdup("localhost");
    config->port = 1833;
    config->username = NULL;
    config->password = NULL;
    config->topic = strdup("test");
    config->qos = 0;
    config->retained = 0;
    config->delimiter = strdup("\n");
    config->client_id = get_client_id();
    config->maximum_length = 2048;
  }
  return config;
}

void config_free(struct config_str *config)
{
  if(config) {
    if(config->hostname) {
      free(config->hostname);
    }
    if(config->username) {
      free(config->username);
    }
    if(config->password) {
      free(config->password);
    }
    if(config->topic) {
      free(config->topic);
    }
    if(config->delimiter) {
      free(config->delimiter);
    }
    if(config->client_id) {
      free(config->client_id);
    }
    if(config->input_file) {
      free(config->input_file);
    }
    free(config);
  }
}

struct config_str *parse_command_line(int argc, char **argv)
{
  char *options = "h:p:q:rd:c:m:u:w:t:?f:";
  char c;
  
  struct config_str *config = config_base();
  if(config) {
    while((c = getopt(argc, argv, options)) != -1) {
      switch(c) {
        case 'h':
          if(config->hostname) {
            free(config->hostname);
          }
          config->hostname = strdup(optarg);
          break;
        case 'p':
          config->port = atoi(optarg);
          if(config->port <= 0) {
            goto bugout;
          }
          break;
        case 'q':
          config->qos = atoi(optarg);
          if(config->qos < 0 || config->qos > 2) {
            goto bugout;
          }
          break;
        case 'r':
          config->retained = 1;
          break;
        case 'd':
          if(config->delimiter) {
            free(config->delimiter);
          }
          config->delimiter = strdup(optarg);
          break;
        case 'c':
          if(config->client_id) {
            free(config->client_id);
          }
          config->client_id = strdup(optarg);
          break;
        case 'm':
          config->maximum_length = atoi(optarg);
          if(config->maximum_length < 0) {
            goto bugout;
          }
          break;
        case 'u':
          if(config->username) {
            free(config->username);
          }
          config->username = strdup(optarg);
          break;
        case 'w':
          if(config->password) {
            free(config->password);
          }
          config->password = strdup(optarg);
          break;
        case 't':
          if(config->topic) {
            free(config->topic);
          }
          config->topic = strdup(optarg);
          break;
        case 'f':
          config->input_file = strdup(optarg);
          break;
        case '?':
          goto bugout;
      }
    }
  }

  if(0) {
bugout:
    if(config) {
      config_free(config);
      config = NULL;
    }
    usage(argv[0]);
  }  
  
  return config;
}

typedef struct mqtt_client_str {  
  // mqtt details
  MQTTClient client;
  MQTTClient_connectOptions connection_opts;
  
  // config details
  int verbose;
} mqtt_client_t;

int mqtt_callback_message_arrived(void* context, char* topicName, 
                                  int topicLen, MQTTClient_message* m)
{
	/* not expecting any messages */
	return 1;
}

MQTTClient_connectOptions connection_opts = MQTTClient_connectOptions_initializer;

mqtt_client_t *mqtt_initialize_client(struct config_str *config)
{
  mqtt_client_t *client = calloc(1, sizeof(mqtt_client_t));
  char *url[512];
  
  if(client) {
    // setup the url
    char *url = calloc(strlen(config->hostname) + 10, sizeof(char));
    snprintf(url, 512, "%s:%d", config->hostname, config->port);
    
    // initialize MQTT client
    MQTTClient_create(&client->client, url, config->client_id, 
                     MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTClient_setCallbacks(client->client, NULL, NULL, 
                            mqtt_callback_message_arrived, NULL);
                           
    // initialize connection settings
    client->connection_opts = connection_opts;
    client->connection_opts.keepAliveInterval = 10;
    client->connection_opts.cleansession = 1;
    client->connection_opts.username = config->username;
    client->connection_opts.password = config->password;
  }
  
  return client;
}

void mqtt_connect(mqtt_client_t* client)
{
	printf("Connecting\n");
	if (MQTTClient_connect(client->client, &client->connection_opts) != 0) {
		printf("Failed to connect\n");
		exit(-1);	
	}
}

int main(int argc, char **argv)
{
  int n, rc;
  json_msg_t *msg = (json_msg_t *)calloc(1, sizeof(json_msg_t));
  
  struct config_str *config = parse_command_line(argc, argv);
  if(config == NULL) {
    usage(argv[0]);
  }
  
  mqtt_client_t *client = mqtt_initialize_client(config);
  mqtt_connect(client);
  
  ds_source_state_t *src = ds_open_file(config->input_file, BUFFER_LENGTH);
  while(1) {
    n = next_message(src, "\n", msg);
    if(n == 0) {
      if(src->eof) {
        // all done with input
        break;
      } else {
        usleep(10000);
      }
    } else {
      rc = MQTTClient_publish(client->client, config->topic, msg->length, msg->body,
                              config->qos, config->retained, NULL);
      if(rc != 0) {
        mqtt_connect(client);
        rc = MQTTClient_publish(client->client, config->topic, msg->length, msg->body,
                                config->qos, config->retained, NULL);
      }        
      reset_json_msg(msg);
    }
  }
  
  MQTTClient_disconnect(client->client, 0);
 	MQTTClient_destroy(&client->client);
 	free(client);
  
  ds_close_file(src);
  free_json_msg(msg);
}


