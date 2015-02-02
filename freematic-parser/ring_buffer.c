#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <stdio.h>

#include "ring_buffer.h"

struct ring_buffer_str {
  void    **buffer;
  int32_t count;
  int32_t size;
  int32_t head;
  int32_t tail;
  
  // data handling functions
  ring_buffer_data_delete_handler delete_handler;
  
  // mutex
  pthread_mutex_t mutex;
};

ring_buffer_t *ring_buffer_create(int size)
{
  ring_buffer_t *ring = (ring_buffer_t *)calloc(1, sizeof(ring_buffer_t));
  if(ring) {
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&ring->mutex, &mattr);    
    
    ring->count = 0;
    ring->size = size;
    ring->head = ring->tail = 0;
    ring->buffer = (void **)calloc(size, sizeof(void *));
    if(!ring->buffer) {
      ring_buffer_destroy(ring);
      ring = NULL;
    }
  }
  return ring;
}

void ring_buffer_destroy(ring_buffer_t *ring)
{
  int i = 0;
  if(ring) {
    if(ring->buffer) {
      if(ring->delete_handler) {
        for(i = 0; i < ring->size; i++) {
          if(ring->buffer[i]) {
            (*ring->delete_handler)(ring->buffer[i]);
            ring->buffer[i] = NULL;
          }
        }
      }
      free(ring->buffer);
    }
    pthread_mutex_destroy(&ring->mutex);
    free(ring);
  }
}

int ring_buffer_available_data(ring_buffer_t *ring)
{
  int result = -1;
  
  if(ring) {
    if(pthread_mutex_trylock(&ring->mutex) != 0) {
      return 0;
    }
    
    result = ring->count;

    pthread_mutex_unlock(&ring->mutex);
  }
  
  return result;
}

int ring_buffer_available_slots(ring_buffer_t *ring)
{
  int result = -1;
  
  if(ring) {
    if(pthread_mutex_trylock(&ring->mutex) != 0) {
      return 0;
    }
    
    result = ring->size - ring->count;    

    pthread_mutex_unlock(&ring->mutex);
  }
  
  return result;
}

int ring_buffer_write(ring_buffer_t *ring, void *data)
{
  int result = -1;
  
  if(ring) {
    if(pthread_mutex_lock(&ring->mutex) != 0) {
      return -1;
    }

    if(ring_buffer_available_slots(ring) > 0) {
      ring->buffer[ring->head++] = data;
      if(ring->head == ring->size) {
        ring->head = 0;
      }
      ring->count++;
      result = 0;
    }

    pthread_mutex_unlock(&ring->mutex);
  }
  
  return result;
}

void *ring_buffer_read(ring_buffer_t *ring)
{
  void *result = NULL;
  
  if(ring) {
    if(pthread_mutex_trylock(&ring->mutex) != 0) {
      return NULL;
    }

    if(ring_buffer_available_data(ring)) {
      result = ring->buffer[ring->tail];
      ring->buffer[ring->tail++] = NULL;
      if(ring->tail == ring->size) {
        ring->tail = 0;
      }
      ring->count--;
    }
  
    pthread_mutex_unlock(&ring->mutex);
  }
  
  return result;
}

void ring_buffer_set_data_delete_method(ring_buffer_t *ring, 
                                        ring_buffer_data_delete_handler handler)
{
  if(ring) {
    ring->delete_handler = handler;
  }
}
