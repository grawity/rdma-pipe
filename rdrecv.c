/*
 * build:
 * cc -o rdrecv rdrecv.c -lrdmacm -libverbs
 *
 * usage:
 * rdrecv <port> <key>
 *
 * Waits for client to connect to given port and authenticate with the key.
 *
 * lib:libibverbs-dev
 */
#include <err.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <arpa/inet.h>

#include <rdma/rdma_cma.h>

// #include "fast_memcpy.h"

enum {
	RESOLVE_TIMEOUT_MS = 5000,
};

struct pdata {
	uint64_t buf_va;
	uint32_t buf_rkey;
};

void usage() {
	fprintf(stderr, "USAGE: rdrecv [-v] <port> <key>\n");
}

int main(int argc, char *argv[])
{
	struct pdata	rep_pdata;

	struct rdma_event_channel   *cm_channel;
	struct rdma_cm_id	    *listen_id;
	struct rdma_cm_id	    *cm_id;
	struct rdma_cm_event	    *event;
	struct rdma_conn_param	    conn_param = { };

	struct ibv_pd		*pd;
	struct ibv_comp_channel *comp_chan;
	struct ibv_cq		*cq;
	struct ibv_cq		*evt_cq;
	struct ibv_mr		*mr;
	struct ibv_qp_init_attr qp_attr = { };
	struct ibv_sge		sge;
	struct ibv_send_wr	send_wr = { };
	struct ibv_send_wr	*bad_send_wr;
	struct ibv_recv_wr	recv_wr = { };
	struct ibv_recv_wr	*bad_recv_wr;
	struct ibv_wc		wc;
	void			*cq_context;

	struct sockaddr_in	sin;

	uint32_t		*buf, *buf2, *tmp;
	uint32_t buf_size = 16 * 524288;


	int			ret;
	uint32_t event_count = 0;

	int port;
	char *key, *cbuf, *ports;
	uint32_t keylen, keyIdx = 0, i = 0;

	int verbose = 0;

	int argv_idx = 1;

	if (argc < 3) {
		usage();
		return 1;
	}

	if (strcmp(argv[argv_idx], "-v") == 0) {
		verbose++;
		argv_idx++;
	}

	ports = argv[argv_idx++];

	port = atoi(ports);

	if (port < 1 || port > 65535)
		errx(2, "port should be 1-65535 (got %d)", port);

	key = argv[argv_idx++];
	keylen = strlen(key);

	/* Set up RDMA CM structures */

	cm_channel = rdma_create_event_channel();
	if (cm_channel == NULL)
		err(1, "could not create rdma event channel");

	ret = rdma_create_id(cm_channel, &listen_id, NULL, RDMA_PS_TCP);
	if (ret < 0)
		err(1, "could not allocate rdma id");

	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = INADDR_ANY;


	/* Bind to local port and listen for connection request */

	ret = rdma_bind_addr(listen_id, (struct sockaddr *) &sin);
	if (ret < 0)
		err(1, "could not bind to local rdma address");

	ret = rdma_listen(listen_id, 1);
	if (ret < 0)
		err(1, "could not listen for rdma connections");

	ret = rdma_get_cm_event(cm_channel, &event);
	if (ret < 0)
		err(1, "could not get rdma CM event");

	if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST)
		errx(1, "unexpected rdma event: %s", rdma_event_str(event->event));

	cm_id = event->id;

	ret = rdma_ack_cm_event(event);
	if (ret < 0)
		err(1, "could not ack rdma CM event");

	/* Create verbs objects now that we know which device to use */

	pd = ibv_alloc_pd(cm_id->verbs);
	if (pd == NULL)
		err(1, "could not allocate protection domain");

	comp_chan = ibv_create_comp_channel(cm_id->verbs);
	if (comp_chan == NULL)
		err(1, "could not create completion channel");

	cq = ibv_create_cq(cm_id->verbs, 2, NULL, comp_chan, 0);
	if (cq == NULL)
		err(1, "could not create completion queue");

	ret = ibv_req_notify_cq(cq, 0);
	if (ret != 0)
		err(1, "could not request completion queue notify");

	buf = calloc(buf_size*2, 1);
	if (buf == NULL)
		err(1, "could not allocate buffers");

	buf2 = ((void*)buf) + buf_size;

	mr = ibv_reg_mr(pd, buf, buf_size*2,
		IBV_ACCESS_LOCAL_WRITE |
		IBV_ACCESS_REMOTE_READ |
		IBV_ACCESS_REMOTE_WRITE);
	if (mr == NULL)
		err(1, "could not register memory region");
		/* TODO: this is where we get ENOMEM if ulimits are too low */

	//qp_attr.cap.max_send_wr = 1;
	//qp_attr.cap.max_send_sge = 1;
	//qp_attr.cap.max_recv_wr = 1;
	//qp_attr.cap.max_recv_sge = 1;

	qp_attr.send_cq = cq;
	qp_attr.recv_cq = cq;

	qp_attr.qp_type = IBV_QPT_RC;

	ret = rdma_create_qp(cm_id, pd, &qp_attr);
	if (ret < 0)
		err(1, "could not create rdma queue pair");

	/* Post receive before accepting connection */
	sge.addr = (uintptr_t) buf2;
	sge.length = buf_size;
	sge.lkey = mr->lkey;

	recv_wr.sg_list = &sge;
	recv_wr.num_sge = 1;

	ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr);
	if (ret != 0)
		err(1, "could not post receive work request");

	rep_pdata.buf_va = htonl((uintptr_t) buf);
	rep_pdata.buf_rkey = htonl(mr->rkey);

	conn_param.responder_resources = 1;
	conn_param.private_data = &rep_pdata;
	conn_param.private_data_len = sizeof rep_pdata;

	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;

	/* Accept connection */

	ret = rdma_accept(cm_id, &conn_param);
	if (ret < 0)
		err(1, "could not accept rdma connection");

	ret = rdma_get_cm_event(cm_channel, &event);
	if (ret < 0)
		err(1, "could not get rdma CM event");

	if (event->event != RDMA_CM_EVENT_ESTABLISHED)
		errx(1, "unexpected rdma event: %s", rdma_event_str(event->event));

	ret = rdma_ack_cm_event(event);
	if (ret < 0)
		err(1, "could not ack rdma CM event");

	while (1) {
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

		/* Flip buffers */

		tmp = buf;
		buf = buf2;
		buf2 = tmp;

		uint32_t msgLen = buf[0];

		if (msgLen > 0) {
			sge.addr = (uintptr_t) buf2;

			ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr);
			if (ret != 0)
				err(1, "could not post receive work request");
		}

		sge.length = 1;
		ret = ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);
		if (ret != 0)
			err(1, "could not post send work request");
		sge.length = buf_size;

		/* Wait for send completion */

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

		// printf("%d\n", msgLen);
		if (msgLen <= buf_size - 4) {
			cbuf = (char*)(&buf[1]);
			//memcpy(buf3, buf, msgLen+4);
			for (i=0; i<msgLen && keyIdx<keylen+1; i++, keyIdx++, cbuf++) {
				// include the zero byte at the end of the key
				if (*cbuf != key[keyIdx]) {
					ibv_ack_cq_events(cq, event_count);
					errx(1, "wrong key was received");
				}
			}
			ret = write(STDOUT_FILENO, cbuf, msgLen-i);
			if (!ret)
				warnx("write error (wrote %d out of %d)", ret, msgLen-1);
		}

		if (msgLen == 0) {
			break;
		}

	}

	close(STDOUT_FILENO);

	ibv_ack_cq_events(cq, event_count);

	return 0;
}
