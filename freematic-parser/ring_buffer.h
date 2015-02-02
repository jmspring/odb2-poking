#ifndef _RING_BUFFER_H_
#define _RING_BUFFER_H_

typedef struct ring_buffer_str ring_buffer_t;

typedef void (*ring_buffer_data_delete_handler)(void *data);

ring_buffer_t *ring_buffer_create(int size);
void ring_buffer_destroy(ring_buffer_t *ring);
void *ring_buffer_read(ring_buffer_t *ring);
int ring_buffer_write(ring_buffer_t *ring, void *data);
int ring_buffer_available_data(ring_buffer_t *ring);
int ring_buffer_available_slots(ring_buffer_t *ring);

void ring_buffer_set_data_delete_method(ring_buffer_t *ring, ring_buffer_data_delete_handler handler);

#endif /* _SIMPLE_RING_BUFFER_H_ */