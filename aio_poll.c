#include <stdlib.h>
#include <malloc.h>
#include <libaio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define goto_on_error(retval, label) { if ((retval) < 0) goto label; }

/* Address to listen on */
const char *addr_to_listen = "127.0.0.1";
/* Port to listen on */
int port_to_listen = 3333;
/* How many connections should an AIO context handle */
int aio_max_conns = 50;
/* Length of listen queue */
int listen_qlen = 5;
/* Read/Write buffer size */
size_t buffer_size = 8192;


/*
 * struct connection:
 *
 * For each connection there is going to be a connection context associated with
 * it.
 */
#define iocb_to_connection(iocb) ((struct connection *)(iocb))
struct connection {
    /* The iocb for polling */
    struct iocb polliocb;
    /* A buffer for both read and write */
    void *buf;
    /* buffer size */
    size_t bufsz;
    /* Pending write size */
    size_t writelen;

    /* Connection context linked-list structure */
    struct connection *prev;
    struct connection *next;
} connection_head = {
    .prev = &connection_head,
    .next = &connection_head
};
size_t connections_cnt = 0;

/*
 * new_connection:
 *
 * Create new connection context based on @fd, @rdbufsz.
 * The successfully created connection will be inserted to connection context
 * linked-list, Otherwise NULL will be returned.
 */
static struct connection *
new_connection(int fd, size_t bufsz)
{
    struct connection *conn;

    conn = malloc(sizeof(struct connection));
    if (conn == NULL)
        return NULL;
    conn->bufsz = bufsz;
    conn->buf = malloc(bufsz);
    if (conn->buf == NULL) {
        free(conn);
        return NULL;
    }
    conn->prev = &connection_head;
    conn->next = connection_head.next;
    conn->next->prev = conn;
    connection_head.next = conn;

    connections_cnt++;
    return conn;
}

/*
 * destroy_connection:
 * 
 * Remove @conn from connection context linked-list, and free the memory
 * associated.
 */
static void
destroy_connection(struct connection *conn)
{
    conn->prev->next = conn->next;
    conn->next->prev = conn->prev;
    free(conn->buf);
    free(conn);

    connections_cnt--;
}

int
main()
{
    int listenfd;
    int maxevents = aio_max_conns + 1;
    io_context_t aioctx = NULL;
    struct iocb listen_iocb;
    struct iocb *iocbp[1];
    struct io_event *ioevents = NULL;

    ioevents = calloc(maxevents, sizeof(struct io_event));

    goto_on_error(listenfd = socket(AF_INET, SOCK_STREAM, 0), on_exit);
    /* Setup an AIO context for later aio-poll usage for one thread */
    goto_on_error(io_setup(maxevents, &aioctx), on_exit);
    /* Try to bind to an address */
    {
        struct sockaddr_in sa;
        sa.sin_family = AF_INET;
        sa.sin_port = htons(port_to_listen);
        sa.sin_addr.s_addr = inet_addr(addr_to_listen);
        goto_on_error(bind(listenfd, (struct sockaddr *)&sa,
            sizeof(struct sockaddr_in)), on_exit);
    }
    /* Listen on the fd */
    goto_on_error(listen(listenfd, listen_qlen), on_exit);
    /* Submit our first poll request for listen fd */
    io_prep_poll(&listen_iocb, listenfd, POLLIN);
    iocbp[0] = &listen_iocb;
    while (io_submit(aioctx, 1, iocbp) < 0) {
        perror("io_submit");
        fprintf(stderr, "AIO submission for fd %d failed, now trying to throttle...\n",
            listenfd);
        sleep(1);
    }

    /* Actual loop */
    while (1) {
        int i;
        int rc;

        /* Try to get available events and block here. Actually this could be
         * done in better ways, such as calling io_getevents() on another thread
         */
        rc = io_getevents(aioctx, 1, maxevents, ioevents, NULL);
        if (rc < 0) {
            fprintf(stderr, "io_getevents: %s\n", strerror(-rc));
            continue;
        } else if (!rc)
            /* Timeout handling */
            continue;

        for (i = 0; i < rc; i++) {
            if (ioevents[i].obj->aio_fildes == listenfd) {
                /* We received incoming connections */
                int flags;
                struct connection *conn;
                struct sockaddr_in inetaddr;
                socklen_t addrlen = sizeof(struct sockaddr_in);
                int acceptfd;

                /* Accept the new incoming connection */
                acceptfd = accept(listenfd, (struct sockaddr *)&inetaddr,
                    &addrlen);
                if (acceptfd < 0) {
                    perror("accept");
                    continue;
                }
                if (connections_cnt) {
                    fprintf(stderr, "Rejected incoming connections %s:%u due to insufficient resources\n",
                        inet_ntoa(inetaddr.sin_addr), ntohs(inetaddr.sin_port));
                    /* Try to be more peaceful in such case. */
                    shutdown(acceptfd, SHUT_RDWR);
                    close(acceptfd);
                    continue;
                }
                fprintf(stderr, "Accepted incoming connections %s:%u\n",
                    inet_ntoa(inetaddr.sin_addr), ntohs(inetaddr.sin_port));

                /* Set the socket to non-blocking */
                flags = fcntl(acceptfd, F_GETFL, 0);
                if (flags == -1) {
                    perror("fcntl");
                    close(acceptfd);
                    continue;
                }
                if (fcntl(acceptfd, F_SETFL, flags|O_NONBLOCK) == -1) {
                    perror("fcntl");
                    close(acceptfd);
                    continue;
                }

                /* Now we get a new connection context for the connection */
                conn = new_connection(acceptfd, buffer_size);
                if (conn == NULL) {
                    fprintf(stderr, "Insufficient memory for setting connection for %s:%u\n",
                        inet_ntoa(inetaddr.sin_addr), ntohs(inetaddr.sin_port));
                    close(acceptfd);
                    continue;
                }

                /* Submit the first poll request to the AIO context for this
                 * connection */
                io_prep_poll(&conn->polliocb, acceptfd, POLLIN);
                iocbp[0] = &conn->polliocb;
                while (io_submit(aioctx, 1, iocbp) < 0) {
                    perror("io_submit");
                    fprintf(stderr, "AIO submission for listen fd %d failed, now trying to throttle...\n",
                        acceptfd);
                    sleep(1);
                }

                /* Submit the next poll request to the AIO context for the
                 * listen fd due to the oneshot nature of the AIO poll
                 * facilities */
                io_prep_poll(&listen_iocb, listenfd, POLLIN);
                iocbp[0] = &listen_iocb;
                while (io_submit(aioctx, 1, iocbp) < 0) {
                    perror("io_submit");
                    fprintf(stderr, "AIO submission for fd %d failed, now trying to throttle...\n",
                        listenfd);
                    sleep(1);
                }
            } else {
                /* IO Logic for the connections */
                if (ioevents[i].res & POLLIN) {
                    /* Handle incoming data */
                    ssize_t rc;
                    int connfd = ioevents[i].obj->aio_fildes;
                    struct connection *conn = iocb_to_connection(ioevents->obj);

                    /* Try to read something... */
                    rc = read(connfd, conn->buf, conn->bufsz);
                    if (rc == -1) {
                        close(connfd);
                        destroy_connection(conn);
                        continue;
                    }
                    if (!rc) {
                        shutdown(connfd, SHUT_RDWR);
                        close(connfd);
                        destroy_connection(conn);
                        continue;
                    }
                    conn->writelen = rc;

                    /* Once we get data in the buffer we are ready to write them
                    * out */
                    io_prep_poll(ioevents[i].obj, connfd, POLLOUT);
                    iocbp[0] = ioevents[i].obj;
                    while (io_submit(aioctx, 1, iocbp) < 0) {
                        perror("io_submit");
                        fprintf(stderr, "AIO submission for fd %d failed, now trying to throttle...\n",
                            listenfd);
                        sleep(1);
                    }
                } else {
                    /* Handle output buffer being ready */
                    ssize_t rc;
                    int connfd = ioevents[i].obj->aio_fildes;
                    struct connection *conn = iocb_to_connection(ioevents->obj);

                    /* Try to write something... */
                    rc = write(connfd, conn->buf, conn->writelen);
                    if (rc == -1) {
                        close(connfd);
                        destroy_connection(conn);
                        continue;
                    }
                    if (rc < conn->writelen) {
                        conn->writelen -= rc;
                        memmove(conn->buf, conn->buf + rc, conn->writelen);

                        /* Now we want to wait for the buffer to be ready before we
                        * send again */
                        io_prep_poll(ioevents[i].obj, connfd, POLLOUT);
                        iocbp[0] = ioevents[i].obj;
                        while (io_submit(aioctx, 1, iocbp) < 0) {
                            perror("io_submit");
                            fprintf(stderr, "AIO submission for fd %d failed, now trying to throttle...\n",
                                listenfd);
                            sleep(1);
                        }
                    } else {
                        /* No more data to send, thus we wait for data ready again
                        */
                        io_prep_poll(ioevents[i].obj, connfd, POLLIN);
                        iocbp[0] = ioevents[i].obj;
                        while (io_submit(aioctx, 1, iocbp) < 0) {
                            perror("io_submit");
                            fprintf(stderr, "AIO submission for fd %d failed, now trying to throttle...\n",
                                listenfd);
                            sleep(1);
                        }
                    }
                }
            }
        }
    }
on_exit:
    /* We should never reach here unless we encounter fatal error */
    if (listenfd != -1)
        close(listenfd);
    if (aioctx != NULL)
        io_destroy(aioctx);
    if (ioevents)
        free(ioevents);

    return EXIT_FAILURE;
}
