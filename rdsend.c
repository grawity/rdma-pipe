/*
 * cc -o rdsend rdsend.c -lrdmacm -libverbs
 *
 * usage:
 * rdsend <server> <port> <key>
 *
 * Reads data from stdin and RDMA sends it to the given server and port, authenticating with the key.
 *
 */
#include <err.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <rdma/rdma_cma.h>

enum   {
	RESOLVE_TIMEOUT_MS = 5000,
};

struct pdata {
	uint64_t	buf_va;
	uint32_t	buf_rkey;
};

void usage() {
	fprintf(stderr, "USAGE: rdsend [-v] <server> <port> <key>\n");
}


int rconnect(char *host, char *port,
	struct rdma_event_channel *cm_channel,
	struct rdma_cm_id **cm_id,
	struct ibv_mr **mr,
	struct ibv_cq **cq,
	struct ibv_pd **pd,
	struct ibv_comp_channel **comp_chan,
	void *buf, uint32_t buf_len,
	struct pdata *server_pdata
) {
	struct rdma_conn_param			conn_param = { };
	struct addrinfo					*res, *t;
	struct addrinfo					hints = {
		.ai_family    = AF_INET,
		.ai_socktype  = SOCK_STREAM
	};
	struct ibv_qp_init_attr			qp_attr = { };
	struct rdma_cm_event			*event;
	int ret;

	ret = rdma_create_id(cm_channel, cm_id, NULL, RDMA_PS_TCP);
	if (ret < 0)
		err(1, "could not allocate rdma id");

	ret = getaddrinfo(host, port, &hints, &res);
	if (ret != 0) {
		warnx("XXX: getaddrinfo failed: %s", gai_strerror(ret));
		errno = ENONET;
		return -1;
	}

	/* Connect to remote end */

	for (t = res; t; t = t->ai_next) {
		ret = rdma_resolve_addr(*cm_id, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS);
		if (ret == 0)
			break;
	}
	if (ret != 0) {
		warn("XXX: could not resolve rdma address");
		errno = ENOLINK;
		return -1;
	}


	ret = rdma_get_cm_event(cm_channel, &event);
	if (ret < 0)
		err(1, "could not get rdma CM event");

	if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
		errx(1, "unexpected rdma event: %s", rdma_event_str(event->event));

	ret = rdma_ack_cm_event(event);
	if (ret < 0)
		err(1, "could not ack rdma CM event");


	ret = rdma_resolve_route(*cm_id, RESOLVE_TIMEOUT_MS);
	if (ret < 0)
		err(1, "could not resolve rdma route"); // XXX: retryable?

	ret = rdma_get_cm_event(cm_channel, &event);
	if (ret < 0)
		err(1, "could not get rdma CM event");

	if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
		errx(1, "unexpected rdma event: %s", rdma_event_str(event->event));

	ret = rdma_ack_cm_event(event);
	if (ret < 0)
		err(1, "could not ack rdma CM event");


	/* Create IB buffers and CQs and things */

	*pd = ibv_alloc_pd((*cm_id)->verbs);
	if (*pd == NULL)
		err(1, "could not allocate protection domain");

	*comp_chan = ibv_create_comp_channel((*cm_id)->verbs);
	if (*comp_chan == NULL)
		err(1, "could not create completion channel");

	*cq = ibv_create_cq((*cm_id)->verbs, 2,NULL, *comp_chan, 0);
	if (*cq == NULL)
		err(1, "could not create completion queue");

	ret = ibv_req_notify_cq(*cq, 0);
	if (ret != 0)
		err(1, "could not request completion queue notify");

	*mr = ibv_reg_mr(*pd, buf, buf_len, IBV_ACCESS_LOCAL_WRITE);
	if (!*mr) {
		if (errno == ENOMEM) {
			warn("XXX: could not register memory region");
			return -1;
		} else {
			err(1, "could not register memory region");
		}
	}

	//qp_attr.cap.max_send_wr = 2;
	//qp_attr.cap.max_send_sge = 1;
	//qp_attr.cap.max_recv_wr = 1;
	//qp_attr.cap.max_recv_sge = 1;

	qp_attr.send_cq        = *cq;
	qp_attr.recv_cq        = *cq;
	qp_attr.qp_type        = IBV_QPT_RC;

	ret = rdma_create_qp(*cm_id, *pd, &qp_attr);
	if (ret < 0)
		err(1, "could not create rdma queue pair");

	conn_param.initiator_depth = 1;
	conn_param.retry_count	   = 7;

	ret = rdma_connect(*cm_id, &conn_param);
	if (ret < 0) {
		warn("XXX: could not create rdma connection");
		return -1;
	}

	/* Connect! */

	ret = rdma_get_cm_event(cm_channel, &event);
	if (ret < 0)
		err(1, "could not get rdma CM event");

	if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
		ret = rdma_ack_cm_event(event);
		if (ret < 0)
			err(1, "could not ack rdma CM event");

		warnx("XXX: rdma_get_cm_event() => %s", rdma_event_str(event->event));
		if (event->event == RDMA_CM_EVENT_REJECTED) {
			warnx("XXX: connection was refused");
			errno = ECONNREFUSED;
			return -1;
			// TODO: this should be fatal too
		} else {
			errx(1, "unexpected rdma event: %s", rdma_event_str(event->event));
		}
	}

	memcpy(server_pdata, event->param.conn.private_data, sizeof(struct pdata));

	ret = rdma_ack_cm_event(event);
	if (ret < 0)
		err(1, "could not ack rdma CM event");

	return 0;
}

int rdisconnect(
	struct rdma_event_channel *cm_channel,
	struct rdma_cm_id *cm_id,
	struct ibv_mr *mr,
	struct ibv_cq *cq,
	struct ibv_pd *pd,
	struct ibv_comp_channel *comp_chan
) {
	int ret;

	ret = rdma_disconnect(cm_id);
	if (ret < 0)
		err(1, "could not close rdma connection");

	rdma_destroy_qp(cm_id);

	ret = ibv_dereg_mr(mr);
	if (ret != 0)
		err(1, "could not deregister memory region");

	ret = ibv_destroy_cq(cq);
	if (ret != 0)
		err(1, "could not destroy completion queue");

	ret = ibv_dealloc_pd(pd);
	if (ret != 0)
		err(1, "could not deallocate protection domain");

	ret = ibv_destroy_comp_channel(comp_chan);
	if (ret != 0)
		err(1, "could not destroy completion channel");

	ret = rdma_destroy_id(cm_id);
	if (ret < 0)
		err(1, "could not release rdma id");

	return 0;
}

int max(int a, int b) {
	return a < b ? b : a;
}

int main(int argc, char   *argv[ ])
{
	struct pdata					server_pdata;

	struct rdma_event_channel		*cm_channel = NULL;
	struct rdma_cm_id				*cm_id = NULL;

	struct ibv_pd					*pd = NULL;
	struct ibv_comp_channel			*comp_chan = NULL;
	struct ibv_cq					*cq = NULL;
	struct ibv_cq					*evt_cq = NULL;
	struct ibv_mr					*mr = NULL;
	struct ibv_sge					sge, rsge;
	struct ibv_send_wr				send_wr = { };
	struct ibv_send_wr				*bad_send_wr;
	struct ibv_recv_wr				recv_wr = { };
	struct ibv_recv_wr				*bad_recv_wr;
	struct ibv_wc					wc;
	void							*cq_context;

	uint32_t						*buf, *buf2, *tmp;

	uint32_t event_count = 0;
	struct timespec now, tmstart;
	double seconds;

	int64_t read_bytes;
	uint64_t total_bytes, buf_read_bytes;
	int wr_id = 1, more_to_send = 1;
	uint32_t buf_size = 16 * (512 * 1024);
	uint32_t buf_len = 0;

	char *host, *ports;
	int port;
	char *key;
	uint32_t keylen;

	int verbose = 0;

	int retries = 0;
	int argv_idx = 1;

	int ret;

	if (argc < 4) {
		usage();
		return 1;
	}

	if (strcmp(argv[argv_idx], "-v") == 0) {
		verbose++;
		argv_idx++;
	}

	host = argv[argv_idx++];
	ports = argv[argv_idx++];

	port = atoi(ports);

	if (port < 1 || port > 65535)
		errx(2, "port should be 1-65535 (got %d)", port);

	key = argv[argv_idx++];
	keylen = strlen(key);

	buf_len = buf_size*2+4;

	struct rlimit lim;
	getrlimit(RLIMIT_MEMLOCK, &lim);
	if (buf_len > lim.rlim_cur) {
		warnx("warning: insufficient RLIMIT_MEMLOCK (want %u, can do %ld:%ld)",
			buf_len, lim.rlim_cur, lim.rlim_max);
	}

	buf = calloc(buf_len, 1);
	if (buf == NULL)
		errx(1, "could not allocate buffers");

	buf2 = (uint32_t*)(((char*)buf) + buf_size);

	/* RDMA CM */
	cm_channel = rdma_create_event_channel();
	if (cm_channel == NULL)
		err(1, "could not create rdma event channel");

	for (;;) {
		int r = rconnect(host, ports, cm_channel, &cm_id, &mr, &cq, &pd, &comp_chan, buf, buf_len, &server_pdata);
		if (r == 0)
			break;
		else if (errno == ENOMEM) {
			warnx("RLIMIT_MEMLOCK insufficient (tried %d bytes)", buf_len);
			if (buf_size > (512 * 1024)) {
				buf_size /= 2;
				buf_len = buf_size*2+4;
				warnx("retrying with %d bytes", buf_len);
			} else {
				return 1;
			}
		} else if (errno == ECONNREFUSED) {
			err(1, "connection failed");
		} else {
			warn("XXX: rconnect failed");
		}

		retries++;
		if (retries > 300)
			errx(1, "giving up connection after 300 retries");

		rdisconnect(cm_channel, cm_id, mr, cq, pd, comp_chan);
		nanosleep((const struct timespec[]){{0, 10000000L}}, NULL);
	}

	/* Prepost */

	sge.addr = (uintptr_t) buf;
	sge.length = buf_size;
	sge.lkey = mr->lkey;

	rsge.addr = (uintptr_t) (((char*)buf) + 2*buf_size);
	rsge.length = 4;
	rsge.lkey = mr->lkey;

	recv_wr.wr_id =     0;
	recv_wr.sg_list =   &rsge;
	recv_wr.num_sge =   1;

	/* Read some bytes from STDIN, send them over with IBV_WR_SEND */

	ret = clock_gettime(CLOCK_REALTIME, &tmstart);
	if (ret < 0)
		err(1, "clock_gettime failed");

	total_bytes = 0;

	memcpy((buf+1), key, keylen+1);

	buf_read_bytes = read_bytes = max(0, read(STDIN_FILENO, ((void*)(buf+1))+keylen+1, buf_size-4-keylen-1)) + keylen + 1;
	// while (read_bytes && buf_read_bytes < buf_size-4) {
	//	read_bytes = read(STDIN_FILENO, ((void*)(buf+1)) + buf_read_bytes, buf_size-4-buf_read_bytes);
	//	buf_read_bytes += read_bytes;
	// }
	// fprintf(stderr, "%d %d\n", read_bytes, errno);
	buf[0] = buf_read_bytes;
	total_bytes += buf_read_bytes;

	more_to_send = 1;

	while (more_to_send) {
		if (buf_read_bytes == 0) {
			if (buf[0] != buf_read_bytes) {
				errx(1, "XXX: buf[0] != buf_read_bytes (%u != %lu)",
					buf[0], buf_read_bytes);
			}
			more_to_send = 0;
		}

		ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr);
		if (ret != 0)
			err(1, "could not post receive work request");

		sge.addr					  = (uintptr_t) buf;
		sge.length		      = buf_read_bytes + 4;
		sge.lkey		      = mr->lkey;

		send_wr.wr_id		      = wr_id;
		send_wr.opcode		      = IBV_WR_SEND;
		send_wr.send_flags	      = IBV_SEND_SIGNALED;
		send_wr.sg_list		      = &sge;
		send_wr.num_sge		      = 1;
		send_wr.wr.rdma.rkey	      = ntohl(server_pdata.buf_rkey);
		send_wr.wr.rdma.remote_addr   = ntohl(server_pdata.buf_va);

		ret = ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);
		if (ret != 0)
			err(1, "could not post send work request");

		tmp = buf;
		buf = buf2;
		buf2 = tmp;

		buf_read_bytes = read_bytes = max(0, read(STDIN_FILENO, &buf[1], buf_size-4));
		// while (read_bytes && buf_read_bytes < buf_size-4) {
		//	read_bytes = read(STDIN_FILENO, ((void*)(&buf[1])) + buf_read_bytes, buf_size-4-buf_read_bytes);
		//	buf_read_bytes += read_bytes;
		// }
		buf[0] = buf_read_bytes;
		// fprintf(stderr, "%d %d %d\n", read_bytes, buf_read_bytes, buf[0]);
		total_bytes += buf_read_bytes;

		/* Wait for a response */

		ret = ibv_get_cq_event(comp_chan, &evt_cq, &cq_context);
		if (ret != 0)
			err(1, "could not get completion queue event");

		ret = ibv_req_notify_cq(cq, 0);
		if (ret != 0)
			err(1, "could not request completion queue notify");

		ret = ibv_poll_cq(cq, 1, &wc);
		if (ret < 1)
			err(1, "could not poll completion queue");

		if (wc.status != IBV_WC_SUCCESS)
			errx(1, "bad wc.status: %s", ibv_wc_status_str(wc.status));

		//printf("%d\n", buf[0]);

		ret = ibv_get_cq_event(comp_chan, &evt_cq, &cq_context);
		if (ret != 0)
			err(1, "could not get completion queue event");

		ret = ibv_req_notify_cq(cq, 0);
		if (ret != 0)
			err(1, "could not request completion queue notify");

		ret = ibv_poll_cq(cq, 1, &wc);
		if (ret < 1)
			err(1, "could not poll completion queue");

		if (wc.status != IBV_WC_SUCCESS)
			errx(1, "bad wc.status: %s", ibv_wc_status_str(wc.status));

		event_count += 2;
		//printf("%d %d %d\n", read_bytes, buf_read_bytes, buf[0]);
	}

	ret = clock_gettime(CLOCK_REALTIME, &now);
	if (ret < 0)
		err(1, "clock_gettime failed");

	seconds = (double)((now.tv_sec+now.tv_nsec*1e-9) - (double)(tmstart.tv_sec+tmstart.tv_nsec*1e-9));
	if (verbose > 0) {
		warnx("bandwidth %.3f GB/s", (total_bytes / seconds) / 1e9);
	}

	ibv_ack_cq_events(cq, event_count);

	rdisconnect(cm_channel, cm_id, mr, cq, pd, comp_chan);

	rdma_destroy_event_channel(cm_channel);

	return 0;
}
