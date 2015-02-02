#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "MQTTAsync.h"

#include "data_stream.h"
#include "ring_buffer.h"

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

MQTTAsync_connectOptions _connectInitializer = MQTTAsync_connectOptions_initializer;
MQTTAsync_responseOptions _publishInitializer = MQTTAsync_responseOptions_initializer;
MQTTAsync_disconnectOptions _disconnectInitializer = MQTTAsync_disconnectOptions_initializer;

typedef struct mqtt_client_str {  
  // mqtt details
  MQTTAsync client;
  MQTTAsync_connectOptions connection_opts;
  MQTTAsync_responseOptions publish_opts;
  MQTTAsync_disconnectOptions disconnect_opts;
  
  // connection details
  int connected;
  int working;
  
  // message info
  ring_buffer_t *message_ring;
  
  // publish details
  json_msg_t *current_message;
  int published;
  int publish_attempts;
  
  // disconnect
  int disconnected;
  
  // config details
  int verbose;
} mqtt_client_t;

void mqtt_callback_connection_lost(void* context, char* cause)
{
  int rc = 0;
	mqtt_client_t *client = (mqtt_client_t *)context;
	if(client) {
    client->connected = 0;
    
    printf("Connecting\n");
    if ((rc = MQTTAsync_connect(client->client, &client->connection_opts)) != MQTTASYNC_SUCCESS) {
      printf("Failed to start connect, return code %d\n", rc);
      exit(-1);	
    }
  }
}

int mqtt_callback_message_arrived(void* context, char* topicName, 
                                  int topicLen, MQTTAsync_message* m)
{
	/* not expecting any messages */
	return 1;
}

void mqtt_callback_on_connect_failure(void* context, MQTTAsync_failureData* response)
{
  printf("Connect failed, rc %d\n", response ? -1 : response->code);
  mqtt_client_t *client = (mqtt_client_t *)context;
  if(client) {
    client->connected = -1;
  }
}

void mqtt_callback_on_connect(void* context, MQTTAsync_successData* response)
{
	printf("Connected\n");
  mqtt_client_t *client = (mqtt_client_t *)context;
  if(client) {
    client->connected = 1;
  }	
}

void mqtt_callback_publish_failure(void* context, MQTTAsync_failureData* response)
{
	printf("Publish failed, rc %d\n", response ? -1 : response->code);
	mqtt_client_t *client = (mqtt_client_t *)context;
	if(client) {
    client->published = -1; 
    client->publish_attempts++;
    client->working = 0;
  }
}

void mqtt_callback_publish_success(void* context, MQTTAsync_successData* response)
{
	mqtt_client_t *client = (mqtt_client_t *)context;
	if(client) {
	static int i = 0;
	fprintf(stderr, "published -- %d\n", i++);
	  client->published = 1;
	  free_json_msg(client->current_message);
	  client->working = 0;
	}
}

mqtt_client_t *mqtt_initialize_client(struct config_str *config, ring_buffer_t *ring)
{
  mqtt_client_t *client = calloc(1, sizeof(mqtt_client_t));
  char *url[512];
  
  if(client) {
    // setup the url
    char *url = calloc(strlen(config->hostname) + 10, sizeof(char));
    snprintf(url, 512, "%s:%d", config->hostname, config->port);
    
    // initialize MQTT client
    MQTTAsync_create(&client->client, url, config->client_id, 
                     MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTAsync_setCallbacks(client->client, client, mqtt_callback_connection_lost, 
                       mqtt_callback_message_arrived, NULL); //, mqtt_callback_message_delivered);
                           
    // initialize connection settings
    client->connection_opts = _connectInitializer;
    client->connection_opts.keepAliveInterval = 10;
    client->connection_opts.cleansession = 1;
    client->connection_opts.username = config->username;
    client->connection_opts.password = config->password;
    client->connection_opts.onSuccess = mqtt_callback_on_connect;
    client->connection_opts.onFailure = mqtt_callback_on_connect_failure;
    client->connection_opts.context = client;
    
    // initialize publish options
    client->publish_opts = _publishInitializer;
    client->publish_opts.onSuccess = mqtt_callback_publish_success;
    client->publish_opts.onFailure = mqtt_callback_publish_failure;
    
    // initialize disconnect options
    client->disconnect_opts = _disconnectInitializer;
    client->disconnected = 0;
    
    // because Paho seems to not be working right --
    client->disconnect_opts.context = client->publish_opts.context = client;
    
    // stats
    client->published = 0;
    client->publish_attempts = 0;
    
    client->connected = 0;
    client->working = 0;
    
    client->message_ring = ring;
  }
  
  return client;
}

void mqtt_connect(mqtt_client_t *client)
{
  int rc;
	if ((rc = MQTTAsync_connect(client->client, &client->connection_opts)) != MQTTASYNC_SUCCESS)
	{
		printf("Failed to start connect, return code %d\n", rc);
		exit(-1);	
	}
}

#define ALLOWED_PUBLISH_ATTEMPTS 5

int main(int argc, char **argv)
{
  int n, rc, wait = 0;
  int done_reading = 0, doing_work = 0;;
  
  struct config_str *config = parse_command_line(argc, argv);
  if(config == NULL) {
    usage(argv[0]);
  }
  
  ring_buffer_t *ring = ring_buffer_create(20);
  mqtt_client_t *client = mqtt_initialize_client(config, ring);
  mqtt_connect(client);
  
  ds_source_state_t *src = ds_open_file(config->input_file, BUFFER_LENGTH);
  json_msg_t *msg = (json_msg_t *)calloc(1, sizeof(json_msg_t));  
  while(1) {
    // check for reads / fill buffer
    doing_work = 0;
    if(ring_buffer_available_slots(client->message_ring) > 0) {
      if(!done_reading) {
        n = next_message(src, "\n", msg);
        if(n == 0) {
          if(src->eof) {
            done_reading = 1;
          }
        } else {
          doing_work = 1;
          ring_buffer_write(client->message_ring, msg);
          msg = NULL;
          msg = (json_msg_t *)calloc(1, sizeof(json_msg_t));
        }
      }
    }

    if(client->connected == 1) {
      if(!client->working) {
        json_msg_t *out_msg = NULL;
        if(client->published == -1) {
          if(client->publish_attempts < ALLOWED_PUBLISH_ATTEMPTS) {
            out_msg = client->current_message;
            client->current_message = NULL;
          } else {
            free_json_msg(client->current_message);
            client->current_message = NULL;
          }
        }
        if(!out_msg) {
          if(ring_buffer_available_data(client->message_ring)) {
            out_msg = (json_msg_t *)ring_buffer_read(client->message_ring);
            client->publish_attempts = 0;
          }
        }
        
        if(out_msg) {
          client->current_message = out_msg;
          client->published = 0;
          client->working = 1;
          rc = MQTTAsync_send(client->client, config->topic, msg->length, msg->body,
                              config->qos, config->retained, &client->publish_opts);
          if(rc != 0) {
            fprintf(stderr, "Error sending message: %d\n", rc);
          }
          doing_work = 1;
        }
      }
    }
    
    if((client->working == 0) && (ring_buffer_available_data(client->message_ring) == 0) && (done_reading)) {
      // we are done
      break;
    }
    
    if(!doing_work) {
      usleep(2000);
    }
  }
  
  rc = MQTTAsync_disconnect(client->client, &client->disconnect_opts);
  if(rc != MQTTASYNC_SUCCESS) {
    fprintf(stderr, "Unable to start disconnect.  Code: %d\n", rc);
    exit(-1);
  }
  
  /*
  
  while(!client->disconnected)
  */
 	MQTTAsync_destroy(&client->client);
 	free(client);
  
  ds_close_file(src);
}


