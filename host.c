#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>

typedef struct schedule_datagram {

	int datagram_classification;
	char *datagram_payload;

} schedule_datagram;

typedef struct hash_datagram {

	int datagram_classification;
	int datagram_timestamp;
	char *datagram_payload;

} hash_datagram;

typedef struct hash_list {

	char *hashed_message;
	struct hash_list *next_hash;

} hash_list;

void run_schedule();
void send_schedule_message();
void *listen_schedule(void *arg);
void *handle_schedule(void *arg);
schedule_datagram parse_schedule(char *datagramv);
char *serialize_schedule(schedule_datagram d);
schedule_datagram construct_schedule_datagram(int c, char *s, int l);

void run_hash();
void send_hash_message();
void *listen_hash(void *arg);
void *handle_hash(void *arg);
hash_datagram parse_hash(char *datagramv);
char *serialize_hash(hash_datagram d);
hash_datagram construct_hash_datagram(int c, char *s, int l);

char *datagram_to_hash(hash_datagram d);
void free_schedule_datagram(schedule_datagram d);
void free_hash_datagram(hash_datagram d);

int check_for_hash(char *h);
void add_hash(char *h);

int compute_verification(int t);

int parse_int(int intv, char *intc);

void print_stats();

int n_hosts; //Number of hosts in DC-net
int n_msgs; //Number of messages to send
int i_host; //Index of host
int t_proto; //Type of communication protocol

int l_key; //Secret key shared with left neighbour
int r_key; //Secret key shared with right neighbour

const int MSG_MAX_LEN = 128; //Standard length of DC-net datagram
const int MSG_ORIGINAL = 1; //Message contains original contents
const int MSG_VERIFY = 2; //Message is verification
const int MSG_ANSWER = 3; //Message is response to verification
const int MSG_REQUEST_KEY = 4; //Message is requesting shared key
const int MSG_ACKNOWLEDGE_KEY = 5; //Message is acknowledging shared key

int round_number; //Round number for scheduling protocol
int next_recipient; //Index of next host to receive message
int verification_count; //Number of verifications received

int o_fd; //Outgoing socket file descriptor
int start_time;
int max_list;

pthread_t listener_thread; //Thread listening to messages

struct sockaddr_in **sin_hosts; //Array of socket addresses of each host
struct hash_list *hashed_messages; //List of hashes of messages sent

int main(int argc, char *argv[]) {

	int i;

	/*
	* Process user arguments
	*
	* First verify user entered two arguments
	*
	* Second parse number of hosts
	*
	* Third parse number of messages
	*
	* Fourth parse the index of the host
	*
	* Fifth parse type of protocol used
	*/

	if(argc != 5) {

		printf("Error: Expected 4 arguments, received %d\n", argc - 1);
		return 0;

	}

	n_hosts = parse_int(0, *(argv + 1));
	n_msgs = parse_int(0, *(argv + 2));
	i_host = parse_int(0, *(argv + 3));
	next_recipient = (i_host + 1) % n_hosts;

	if(i_host % 2 == 0) {

		l_key = 0;
		r_key = 1;

	} else {

		l_key = 1;
		r_key = 0;

	}

	if(strcmp(*(argv + 4), "schedule") == 0) {

		t_proto = 1;


	} else if(strcmp(*(argv + 4), "hash") == 0) {

		t_proto = 2;

	} else {

		printf("Error: Unrecognized protocol: %s\n", *(argv + 4));
		return 0;

	}

	/*
	* Parse list of host ports
	*/

	sin_hosts = (struct sockaddr_in**) malloc(2 * n_hosts * sizeof(struct sockaddr_in*));

	for(i = 0; i < n_hosts * 2; i++) {

		int host_port;
		struct sockaddr_in *sin_host;

		host_port = 30000 + i;
		sin_host = (struct sockaddr_in*) malloc(sizeof(struct sockaddr_in));

		memset(sin_host, 0, sizeof(struct sockaddr_in));

		sin_host->sin_family = AF_INET;
		sin_host->sin_port = htons(host_port);
		sin_host->sin_addr.s_addr = inet_addr("0.0.0.0");

		*(sin_hosts + i) = sin_host;

	}

	if(t_proto == 1) {

		run_schedule();

	} else if(t_proto == 2) {

		run_hash();

	}
	
	return 0;

}

/*
* This function simulates a host implementing the scheduling protocol
*/

void run_schedule() {

	pthread_t listener_t;
	struct timeval t;

	pthread_create(&listener_t, NULL, &listen_schedule, NULL);

	round_number = 0;
	verification_count = 0;

	o_fd = socket(AF_INET, SOCK_DGRAM, 0);

	gettimeofday(&t, NULL);

	start_time = t.tv_usec;
	max_list = 0;

	if(o_fd < 0) {

		printf("Error: Unable to create outgoing socket!\n");
		return;

	} else {

		printf("Info: Outgoing socket created!\n");

	}

	if(bind(o_fd, (struct sockaddr*) *(sin_hosts + 2 * i_host + 1), sizeof(**(sin_hosts + 2 * i_host + 1))) < 0) {

		printf("Error: Unable to bind outgoing socket!\n");
		return;

	} else {

		printf("Info: Outgoing socket binded successfully!\n");

	}

	printf("Info: Sending from port %d...\n", ntohs((*(sin_hosts + 2 * i_host + 1))->sin_port));

	if(i_host == 0) {

		send_schedule_message();
		round_number++;

	}

	listener_thread = listener_t;

	pthread_join(listener_t, NULL);

}

void send_schedule_message() {

	struct sockaddr_in server_addr;
	server_addr = **(sin_hosts + next_recipient * 2);
	char *hello = serialize_schedule(construct_schedule_datagram(MSG_ORIGINAL, "hi", 3));

	verification_count = 0;

	printf("Info: Sending message\n");

	sendto(o_fd, (const char *)hello, strlen(hello),
	MSG_CONFIRM, (const struct sockaddr *) &server_addr,
		sizeof(server_addr));

	next_recipient = (next_recipient + 1) % n_hosts;

	if(next_recipient == i_host) {

		next_recipient = (next_recipient + 1) % n_hosts;

	}

}

/*
* This function listens for messages and offloads tasks to
* a child thread once a message is received
*/

void *listen_schedule(void *arg) {

	int s_fd;
	char *s_buf;
	int s_len;
	int n;

	s_buf = (char*) malloc(MSG_MAX_LEN * sizeof(char));
	s_fd = socket(AF_INET, SOCK_DGRAM, 0);

	if(s_fd < 0) {

		printf("Error: Unable to create incoming socket!\n");
		return;

	} else {

		printf("Info: Incoming socket created successfully!\n");

	}

	if(bind(s_fd, (struct sockaddr*) *(sin_hosts + 2 * i_host), sizeof(**(sin_hosts + 2 * i_host))) < 0) {

		printf("Error: Unable to bind incoming socket!\n");
		return;

	} else {

		printf("Info: Incoming socket binded successfully!\n");

	}

	printf("Info: Listening on port %d...\n", ntohs((*(sin_hosts + 2 * i_host))->sin_port));

	while(1) {

		pthread_t worker_t;

		n = recvfrom(s_fd, s_buf, MSG_MAX_LEN, MSG_WAITALL, NULL, &s_len);
		pthread_create(&worker_t, NULL, &handle_schedule, (void*) s_buf);

	}

}

void *handle_schedule(void *arg) {

	char *argv;
	schedule_datagram dg;

	argv = (char*) arg;
	dg = parse_schedule(argv);

	if(dg.datagram_classification == MSG_ORIGINAL) {

		schedule_datagram r_dg = construct_schedule_datagram(MSG_VERIFY, "", 1);
		int peer_i;

		for(peer_i = 0; peer_i < n_hosts; peer_i++) {

			if(peer_i == i_host) {

				continue;

			}

			struct sockaddr_in peer_socket;
			peer_socket = **(sin_hosts + peer_i * 2);
			char *response = serialize_schedule(r_dg);

			sendto(o_fd, response, strlen(response),
				MSG_CONFIRM, (const struct sockaddr *) &peer_socket,
					sizeof(peer_socket));

		}

		printf("Info: Message received\n");

		round_number++;

		if(round_number == n_hosts * n_msgs) {

			print_stats();

		}

		if(round_number % n_hosts == i_host) {

			send_schedule_message();

		}

	} else if(dg.datagram_classification == MSG_VERIFY) {

		schedule_datagram r_dg;
		int peer_i;

		if(round_number % n_hosts == i_host) {

			r_dg = construct_schedule_datagram(MSG_ANSWER, "yes", 4);
			printf("Info: Confirmed message\n");

		} else {

			r_dg = construct_schedule_datagram(MSG_ANSWER, "no", 3);
			printf("Info: Denied message\n");

		}

		for(peer_i = 0; peer_i < n_hosts; peer_i++) {

			if(peer_i == i_host) {

				continue;

			}

			struct sockaddr_in peer_socket;
			peer_socket = **(sin_hosts + peer_i * 2);
			char *response = serialize_schedule(r_dg);

			sendto(o_fd, response, strlen(response),
				MSG_CONFIRM, (const struct sockaddr *) &peer_socket,
					sizeof(peer_socket));

		}

		round_number++;

	} else if(dg.datagram_classification == MSG_ANSWER) {

		verification_count++;

		if(verification_count == n_hosts - 1) {

			printf("Info: Validated message\n");

		}

	}



}

/*
* This function parses a message as a datagram
*/

schedule_datagram parse_schedule(char *datagramv) {

	schedule_datagram d;
	int i;

	d.datagram_classification = 0 | *(datagramv + 0);
	d.datagram_payload = (char*) malloc((MSG_MAX_LEN - 1) * sizeof(char));

	for(i = 1; i < MSG_MAX_LEN; i++) {

		*(d.datagram_payload + i - 1) = *(datagramv + i);

	}

	return d;

}

/*
* This function serializes a datagram as a message
*/

char *serialize_schedule(schedule_datagram d) {

	char *s;
	int i;

	s = (char*) malloc(MSG_MAX_LEN * sizeof(char));

	*(s + 0) = (d.datagram_classification >> 0) & 0xFF;

	for(i = 1; i < MSG_MAX_LEN; i++) {

		*(s + i) = *(d.datagram_payload + i - 1);

	}

	return s;

}

/*
* Create and format a hash_datagram struct
*/

schedule_datagram construct_schedule_datagram(int c, char *s, int l) {

	schedule_datagram d;
	char *p;
	int i;

	p = (char*) malloc((MSG_MAX_LEN - 1) * sizeof(char));
	memset(p, 0, (MSG_MAX_LEN - 1) * sizeof(char));

	for(i = 0; i < l; i++) {

		*(p + i) = *(s + i);

	}

	d.datagram_classification = c;
	d.datagram_payload = p;

	return d;

}

/*
* This function simulates a host implementing the hashing protocol
*/

void run_hash() {

	pthread_t listener_t;
	struct timeval t;

	hashed_messages = NULL;

	pthread_create(&listener_t, NULL, &listen_hash, NULL);

	round_number = 0;
	verification_count = 0;

	o_fd = socket(AF_INET, SOCK_DGRAM, 0);

	gettimeofday(&t, NULL);

	start_time = t.tv_usec;
	max_list = 0;

	if(o_fd < 0) {

		printf("Error: Unable to create outgoing socket!\n");
		return;

	} else {

		printf("Info: Outgoing socket created!\n");

	}

	if(bind(o_fd, (struct sockaddr*) *(sin_hosts + 2 * i_host + 1), sizeof(**(sin_hosts + 2 * i_host + 1))) < 0) {

		printf("Error: Unable to bind outgoing socket!\n");
		return;

	} else {

		printf("Info: Outgoing socket binded successfully!\n");

	}

	printf("Info: Sending from port %d...\n", ntohs((*(sin_hosts + 2 * i_host + 1))->sin_port));

	if(i_host == 0) {

		send_schedule_message();
		round_number++;

	}

	listener_thread = listener_t;

	pthread_join(listener_t, NULL);

}

void send_hash_message() {

	struct sockaddr_in server_addr;
	hash_datagram dh = construct_hash_datagram(MSG_ORIGINAL, "hi", 3);
	server_addr = **(sin_hosts + next_recipient * 2);
	char *hello = serialize_hash(dh);

	add_hash(datagram_to_hash(dh));

	verification_count = 0;

	printf("Info: Sending message\n");

	sendto(o_fd, (const char *)hello, strlen(hello),
	MSG_CONFIRM, (const struct sockaddr *) &server_addr,
		sizeof(server_addr));

	next_recipient = (next_recipient + 1) % n_hosts;

	if(next_recipient == i_host) {

		next_recipient = (next_recipient + 1) % n_hosts;

	}

}

/*
* This function listens for messages and offloads tasks to
* a child thread once a message is received
*/

void *listen_hash(void *arg) {

	int s_fd;
	char *s_buf;
	int s_len;
	int n;

	s_buf = (char*) malloc(MSG_MAX_LEN * sizeof(char));
	s_fd = socket(AF_INET, SOCK_DGRAM, 0);

	if(s_fd < 0) {

		printf("Error: Unable to create incoming socket!\n");
		return;

	} else {

		printf("Info: Incoming socket created successfully!\n");

	}

	if(bind(s_fd, (struct sockaddr*) *(sin_hosts + 2 * i_host), sizeof(**(sin_hosts + 2 * i_host))) < 0) {

		printf("Error: Unable to bind incoming socket!\n");
		return;

	} else {

		printf("Info: Incoming socket binded successfully!\n");

	}

	printf("Info: Listening on port %d...\n", ntohs((*(sin_hosts + 2 * i_host))->sin_port));

	while(1) {

		pthread_t worker_t;

		n = recvfrom(s_fd, s_buf, MSG_MAX_LEN, MSG_WAITALL, NULL, &s_len);
		pthread_create(&worker_t, NULL, &handle_hash, (void*) s_buf);

	}

}

void *handle_hash(void *arg) {

	char *argv;
	hash_datagram dg;

	argv = (char*) arg;
	dg = parse_hash(argv);

	if(dg.datagram_classification == MSG_ORIGINAL) {

		hash_datagram r_dg = construct_hash_datagram(MSG_VERIFY, "", 1);
		int peer_i;

		for(peer_i = 0; peer_i < n_hosts; peer_i++) {

			if(peer_i == i_host) {

				continue;

			}

			struct sockaddr_in peer_socket;
			peer_socket = **(sin_hosts + peer_i * 2);
			char *response = serialize_hash(r_dg);

			sendto(o_fd, response, strlen(response),
				MSG_CONFIRM, (const struct sockaddr *) &peer_socket,
					sizeof(peer_socket));

		}

		printf("Info: Message received\n");

		round_number++;

		if(round_number == n_hosts * n_msgs) {

			print_stats();

		}

		if(round_number % n_hosts == i_host) {

			send_hash_message();

		}

	} else if(dg.datagram_classification == MSG_VERIFY) {

		hash_datagram r_dg;
		int peer_i;

		if(round_number % n_hosts == i_host) {

			r_dg = construct_hash_datagram(MSG_ANSWER, "yes", 4);
			printf("Info: Confirmed message\n");

		} else {

			r_dg = construct_hash_datagram(MSG_ANSWER, "no", 3);
			printf("Info: Denied message\n");

		}

		for(peer_i = 0; peer_i < n_hosts; peer_i++) {

			if(peer_i == i_host) {

				continue;

			}

			struct sockaddr_in peer_socket;
			peer_socket = **(sin_hosts + peer_i * 2);
			char *response = serialize_hash(r_dg);

			sendto(o_fd, response, strlen(response),
				MSG_CONFIRM, (const struct sockaddr *) &peer_socket,
					sizeof(peer_socket));

		}

		round_number++;

	} else if(dg.datagram_classification == MSG_ANSWER) {

		verification_count++;

		if(verification_count == n_hosts - 1) {

			printf("Info: Validated message\n");

		}

	}

}

/*
* This function serializes a datagram as a message
*/

hash_datagram parse_hash(char *datagramv) {

	hash_datagram d;
	int i;
	int ts;


	ts = 0;
	ts = *(datagramv + 1) << 8;
	ts = ts | *(datagramv + 2);
	ts = ts & 0xFFFF;

	d.datagram_classification = 0 | *(datagramv + 0);
	d.datagram_timestamp = ts;
	d.datagram_payload = malloc((MSG_MAX_LEN - 3) * sizeof(char));

	for(i = 3; i < MSG_MAX_LEN; i++) {

		*(d.datagram_payload + i - 3) = *(datagramv + i);

	}

	return d;

}

/*
* This function serializes a datagram as a message
*/

char *serialize_hash(hash_datagram d) {

	char *s;
	int i;

	s = (char*) malloc(MSG_MAX_LEN * sizeof(char));

	*(s + 0) = (d.datagram_classification >> 0) & 0xFF;
	*(s + 1) = (d.datagram_timestamp >> 8) & 0xFF;
	*(s + 2) = (d.datagram_timestamp >> 0) & 0xFF;

	for(i = 3; i < MSG_MAX_LEN; i++) {

		*(s + i) = *(d.datagram_payload + i - 3);

	}

	return s;

}

/*
* Create and format a hash_datagram struct
*/

hash_datagram construct_hash_datagram(int c, char *s, int l) {

	hash_datagram d;
	char *p;
	int i;
	struct timespec t;
	int ts;
	char *ser;

	clock_gettime(CLOCK_REALTIME, &t);

	p = (char*) malloc((MSG_MAX_LEN - 3) * sizeof(char));
	memset(p, 0, (MSG_MAX_LEN - 3) * sizeof(char));

	for(i = 0; i < l; i++) {

		*(p + i) = *(s + i);

	}

	ts = (int) t.tv_nsec;

	d.datagram_classification = c;
	d.datagram_timestamp = ts & 0xFFFF;
	d.datagram_payload = p;

	ser = serialize_hash(d);
	d = parse_hash(ser);
	free(ser);

	return d;

}

/*
* This function hashes a datagram
*/

char *datagram_to_hash(hash_datagram d) {

	char *s_hash;
	char *s_datagram;

	int i;

	s_hash = (char*) malloc((MSG_MAX_LEN / 2) * sizeof(char));
	memset(s_hash, 0, (MSG_MAX_LEN / 2) * sizeof(char));
	s_datagram = serialize_hash(d);

	for(i = 0; i < MSG_MAX_LEN / 2; i++) {

		*(s_hash + i) = *(s_datagram + 2 * i);

	}

	free(s_datagram);

	return s_hash;

}

/*
* This function frees the contents of a schedule_datagram struct
*/

void free_schedule_datagram(schedule_datagram d) {

	free(d.datagram_payload);

}

/*
* This function frees the contents of a hash_datagram struct
*/

void free_hash_datagram(hash_datagram d) {

	free(d.datagram_payload);

}

/*
* This function checks if host sent a message with hash h
* Returns 1 and removes hash if sent, otherwise 0
*/

int check_for_hash(char *h) {

	hash_list *hl;
	hash_list *ph;

	hl = hashed_messages;
	ph = NULL;

	while(hl != NULL) {

		if(strcmp(hl->hashed_message, h) == 0) {

			if(ph == NULL) {

				hashed_messages = hashed_messages->next_hash;

			} else {

				ph->next_hash = hl->next_hash;

				//free(hl->hashed_message);
				free(hl);

			}

			return 1;

		} else {

			ph = hl;
			hl = hl->next_hash;

		}

	}

	return 0;

}

/*
* Append hash to list of hashed messages
*/

void add_hash(char *h) {

	int height;

	hash_list *hl;
	hash_list *ch;

	height = 0;

	hl = hashed_messages;

	ch = (hash_list*) malloc(sizeof(hash_list));
	ch->next_hash = NULL;	
	ch->hashed_message = h;

	if(hl == NULL) {

		hashed_messages = ch;

	} else {

		height = 1;

		while(hl->next_hash != NULL) {

			height++;

			hl = hl->next_hash;

		}

		hl->next_hash = ch;

		if(height > max_list) {

			max_list = height;

		}

	}

}

/*
* This function parses a string as an integer
*/

int parse_int(int intv, char *intc) {

	if(*intc == 0) {

		return intv;

	} else {

		return parse_int(10 * intv + *intc - '0', intc + 1);

	}

}

/*
* Print runtime and memory usage
*/

void print_stats() {

	struct timeval t;
	int end_time;
	long duration;

	printf("Info: Printing stats...\n");

	gettimeofday(&t, NULL);

	end_time = t.tv_usec;

	duration = end_time - start_time;

	printf("Info: Duration - %dms\n", (duration / 1000));
	printf("Info: Max list size - %dB\n", max_list * 64);

}
