/*      Replication server - shane.warner@fox.com
        - Polls the "Last Update" times on an RW vs it's RO volumes and releases them if needed.
        - Threaded and will release multiple volumes in parallel.
        - Queries VLDB for volume status before attempting a release. (Will skip a release on LOCKED volume).
 */

/* Defaults */
/* Max threads, how many volumes to release in parallel */
#define MAXTHREADS 4

/* Time in seconds to poll for volume update times  */
#define POLLTIME 60

/* Hard limit of maximum threads. This supercedes MAXTHREADS.*/
#define THREADMAX 8

static int polltime = POLLTIME;
int maxthreads = MAXTHREADS;

#include <afsconfig.h>
#include <afs/param.h>

#ifdef IGNORE_SOME_GCC_WARNINGS
#pragma GCC diagnostic warning "-Wimplicit-function-declaration"
#endif

#include <sys/types.h>
#include <string.h>
#include <sys/time.h>
#include <sys/file.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>

#include <lock.h>
#include <afs/stds.h>
#include <rx/xdr.h>
#include <rx/rx.h>
#include <rx/rx_globals.h>
#include <afs/nfs.h>
#include <afs/vlserver.h>
#include <afs/cellconfig.h>
#include <afs/keys.h>
#include <afs/afsutil.h>
#include <ubik.h>
#include <afs/afsint.h>
#include <afs/cmd.h>
#include <afs/usd.h>
#include "volser.h"
#include "volint.h"
#include <afs/ihandle.h>
#include <afs/vnode.h>
#include <afs/volume.h>
#include <afs/com_err.h>
#include "dump.h"
#include "lockdata.h"

#include "volser_internal.h"
#include "volser_prototypes.h"
#include "vsutils_prototypes.h"
#include "lockprocs_prototypes.h"

#define QUEUE_INITIALIZER(buffer) { buffer, sizeof(buffer) / sizeof(buffer[0]), 0, 0, 0, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, PTHREAD_COND_INITIALIZER }

const int buffer_size = 64;
int keepRunning = 1;

typedef struct queue {
    void **buffer;
    const int capacity;
    int size;
    int in;
    int out;
    pthread_mutex_t mutex;
    pthread_cond_t cond_full;
    pthread_cond_t cond_empty;
} queue_t;

void queue_enqueue(queue_t *queue, void *value) {
    pid_t tid;
    tid = syscall(SYS_gettid);

    pthread_mutex_lock(&(queue->mutex));
    while (queue->size == queue->capacity)
        pthread_cond_wait(&(queue->cond_full), &(queue->mutex));
    queue->buffer[queue->in] = value;
    ++queue->size;
    ++queue->in;
    queue->in %= queue->capacity;
    pthread_mutex_unlock(&(queue->mutex));
    pthread_cond_broadcast(&(queue->cond_empty));
}

void *queue_dequeue(queue_t *queue) {
    pthread_mutex_lock(&(queue->mutex));
    while(queue->size == 0)
        pthread_cond_wait(&(queue->cond_empty), &(queue->mutex));
    void *value = queue->buffer[queue->out];
    --queue->size;
    ++queue->out;
    queue->out %= queue->capacity;
    pthread_mutex_unlock(&(queue->mutex));
    pthread_cond_broadcast(&(queue->cond_full));
    return value;
}

#ifdef HAVE_POSIX_REGEX
#include <regex.h>
#endif

struct sigs {
    char *type;
    int num;
} types[] = {
    {"SIGINT", 2},
    {"SIGQUIT", 3},
    {"SIGILL", 4},
    {"SIGTRAP", 5},
    {"SIGABRT", 6},
    {"SIGEMT", 7},
    {"SIGFPE", 8},
    {"SIGKILL", 9},
    {"SIGBUS", 10},
    {"SIGSEGV", 11},
    {"SIGSYS", 12},
    {"SIGPIPE", 13},
    {"SIGALRM", 14},
    {"SIGTERM", 15},
    {"SIGUSR1", 16},
    {"SIGUSR2", 17},
};

/* Local Prototypes */
int GetVolumeInfo(afs_uint32 volid, afs_uint32 *server, afs_int32 *part,
        afs_int32 *voltype, struct nvldbentry *rentry);

struct tqElem {
    afs_uint32 volid;
    struct tqElem *next;
};

struct tqHead {
    afs_int32 count;
    struct tqElem *next;
};

#define ERROR_EXIT(code) do { \
    error = (code); \
    goto error_exit; \
} while (0)

int rxInitDone = 0;
struct rx_connection *tconn;
extern struct ubik_client *cstruct;
const char *confdir;

void signal_handler(int signum) {
    pid_t tid;
    tid = syscall(SYS_gettid);

    fprintf(STDERR, "repserver[%d]: Caught signal[%i]. Signaling children to exit.\n", tid, signum);
    keepRunning = 0;
}

static int
ubikConnect(void) {
    char *tcell;
    afs_int32 code;
    afs_int32 sauth;

    /* Initialize the ubik_client connection */
    rx_SetRxDeadTime(90);
    cstruct = (struct ubik_client *) 0;
    sauth = 1;
    tcell = NULL;

    if ((code = vsu_ClientInit(0, confdir, tcell, sauth, &cstruct, UV_SetSecurity))) {
        fprintf(STDERR, "could not initialize VLDB library (code=%lu) \n",
                (unsigned long) code);
        return -1;
    }

    rxInitDone = 1;
    verbose = 0;
    noresolve = 0;

    return 0;
}

static int
ReleaseVolume(afs_uint32 id) {
    struct nvldbentry entry;
    afs_uint32 avolid;
    afs_uint32 aserver;
    afs_int32 apart, vtype, code;
    int force = 0;
    int stayUp = 1;
    pid_t tid;
    tid = syscall(SYS_gettid);
    avolid = id;

    if (avolid == 0) {
        fprintf(STDERR, "repserver[%d]: can't find volume '%lu'.", tid, (unsigned long) avolid);
        return -1;
    }

    code = GetVolumeInfo(avolid, &aserver, &apart, &vtype, &entry);
    if (code)
        return code;

    if (vtype != RWVOL) {
        fprintf(STDERR, "repserver[%d]: %s not a RW volume.", tid, entry.name);
        return -1;
    }

    if (!ISNAMEVALID(entry.name)) {
        fprintf(STDERR, "repserver[%d]: Volume name %s is too long, rename before releasing.", tid, entry.name);
        return -1;
    }

    code = UV_ReleaseVolume(avolid, aserver, apart, force, stayUp);

    if (code) {
        fprintf(STDERR, "repserver[%d]: %d", tid, code);
        return -1;
    }

    fprintf(STDERR, "repserver[%d]: Released volume[%lu] successfully\n\n", tid, (unsigned long) avolid);
    return 0;
}

/* set <server> and <part> to the correct values depending on
 * <voltype> and <entry> */
static void
GetServerAndPart(struct nvldbentry *entry, int voltype, afs_uint32 *server,
        afs_int32 *part, int *previdx) {
    int i, istart, vtype;

    *server = -1;
    *part = -1;

    /* Doesn't check for non-existance of backup volume */
    if ((voltype == RWVOL) || (voltype == BACKVOL)) {
        vtype = ITSRWVOL;
        istart = 0; /* seach the entire entry */
    } else {
        vtype = ITSROVOL;
        /* Seach from beginning of entry or pick up where we left off */
        istart = ((*previdx < 0) ? 0 : *previdx + 1);
    }

    for (i = istart; i < entry->nServers; i++) {
        if (entry->serverFlags[i] & vtype) {
            *server = entry->serverNumber[i];
            *part = entry->serverPartition[i];
            *previdx = i;
            return;
        }
    }

    /* Didn't find any, return -1 */
    *previdx = -1;
#ifdef DEBUG
    fprintf(STDERR, "repserver: GetServerAndPart(): line 171: Didn't find any!\n\n");
#endif
    return;
}

int GetVolumeInfo(afs_uint32 volid, afs_uint32 *server, afs_int32 *part,
        afs_int32 *voltype, struct nvldbentry *rentry) {
    afs_int32 vcode;
    int i, index = -1;

    vcode = VLDB_GetEntryByID(volid, -1, rentry);
    if (vcode) {
        fprintf(STDERR,
                "Could not fetch the entry for volume %lu from VLDB \n\n",
                (unsigned long) (volid));
        PrintError("", vcode);
        return (vcode);
    }
    MapHostToNetwork(rentry);
    if (volid == rentry->volumeId[ROVOL]) {
        *voltype = ROVOL;
        for (i = 0; i < rentry->nServers; i++) {
            if ((index == -1) && (rentry->serverFlags[i] & ITSROVOL)
                    && !(rentry->serverFlags[i] & RO_DONTUSE))
                index = i;
        }
        if (index == -1) {
            fprintf(STDERR,
                    "RO volume is not found in VLDB entry for volume %lu\n\n",
                    (unsigned long) (volid));
            return -1;
        }

        *server = rentry->serverNumber[index];
        *part = rentry->serverPartition[index];
        return 0;
    }

    index = Lp_GetRwIndex(rentry);
    if (index == -1) {
        fprintf(STDERR,
                "RW Volume is not found in VLDB entry for volume %lu\n\n",
                (unsigned long) (volid));
        return -1;
    }
    if (volid == rentry->volumeId[RWVOL]) {
        *voltype = RWVOL;
        *server = rentry->serverNumber[index];
        *part = rentry->serverPartition[index];
        return 0;
    }
    if (volid == rentry->volumeId[BACKVOL]) {
        *voltype = BACKVOL;
        *server = rentry->serverNumber[index];
        *part = rentry->serverPartition[index];
        return 0;
    }
    fprintf(STDERR,
            "unexpected volume type for volume %lu\n\n",
            (unsigned long) (volid));
    return -1;
}

/*------------------------------------------------------------------------
 * PRIVATE SyncStatus
 *
 * Description:
 *	Routine that determines whether a volume is in sync vs
 *              it's RO's.
 *
 * Arguments:
 *	volumename : Ptr to volume name.
 *
 * Returns:
 *	0 for a successful operation,
 *	-1 on failure.
 *
 * Environment:
 *	Nothing interesting.
 *
 * Side Effects:
 *	As advertised.
 *------------------------------------------------------------------------
 */
static int SyncStatus(afs_uint32 volid) {
    struct nvldbentry entry;
    afs_int32 vcode = 0;
    volintInfo *pntr = (volintInfo *) 0;
    afs_int32 code, err, error = 0;
    int voltype, foundserv = 0, foundentry = 0;
    afs_uint32 aserver;
    afs_int32 apart;
    int previdx = -1;
    int wantExtendedInfo = 0; /*Do we want extended vol info? */
    int ROtally = 0;
    char tmpname[32];
    time_t rw, ro;
    pid_t tid;
    tid = syscall(SYS_gettid);
    wantExtendedInfo = 0; /* -extended */

    /* RW VOLUME */

    if (volid == 0) {
        fprintf(STDERR, "repserver[%d]: Unknown volume ID or name: %lu\n\n", tid, (unsigned long) volid);
        return 0;
    }

    vcode = VLDB_GetEntryByID(volid, -1, &entry);
    if (vcode) {
        fprintf(STDERR, "repserver[%d]: Could not fetch the entry for volume number: %lu\n\n", tid, (unsigned long) volid);
        return 0;
    }

    MapHostToNetwork(&entry);

    GetServerAndPart(&entry, 0, &aserver, &apart, &previdx);

    code = UV_ListOneVolume(aserver, apart, volid, &pntr);

    if (code) {
        error = code;
        if (code == ENODEV) {
            /* No device found on disk*/
            return 0;
        } else {
            return 0;
        }
    }

    rw = pntr->updateDate;
    if (pntr)
        free(pntr);

    /* END RW VOLUME */

    /* RO VOLUME INIT */

    snprintf(tmpname, sizeof (tmpname), "%s.readonly", entry.name);

    volid = vsu_GetVolumeID(tmpname, cstruct, &err);
    if (volid == 0) {
        if (err) {
            fprintf(STDERR, "repserver[%d]: Volume: %lu Error: %d\n\n", tid, (unsigned long) volid, err);
            return 0;
        } else {
            fprintf(STDERR, "repserver[%d]: Unknown volume ID or name: %lu\n\n", tid, (unsigned long) volid);
            return 0;
        }
    }

    vcode = VLDB_GetEntryByID(volid, -1, &entry);
    if (vcode) {
        fprintf(STDERR, "repserver[%d]: Could not fetch the entry for volume: %s, volume number: %lu\n\n", tid, tmpname, (unsigned long) volid);
        return 0;
    }

    MapHostToNetwork(&entry);

    voltype = ROVOL;
    /* END RO VOLUME INIT */

    do { /* do {...} while (voltype == ROVOL) */
        /* Get the entry for the volume. If its a RW vol, get the RW entry.
         * It its a BK vol, get the RW entry (even if VLDB may say the BK doen't exist).
         * If its a RO vol, get the next RO entry.
         */

        GetServerAndPart(&entry, 1,
                &aserver, &apart, &previdx);
        if (previdx == -1) { /* searched all entries */
            if (!foundentry) {
                /* This error message is more of an annoyance with constant polling. Commented out for now. */
                /* fprintf(STDERR,"Volume has no read replicas: %s\n\n", volumename); */
                error = ENOENT;
                return 0;
            }
            break;
        }
        foundentry = 1;

        /* Get information about the volume from the server */
        code = UV_ListOneVolume(aserver, apart, volid, &pntr);

        if (code) {
            error = code;
            if (code == ENODEV) {
                if ((voltype == BACKVOL) && !(entry.flags & BACK_EXISTS)) {
                    /* The VLDB says there is no backup volume and its not on disk */
                    fprintf(STDERR, "repserver[%d]: No backup volume in VLDB or on disk: %lu\n\n", tid, (unsigned long) volid);
                    error = ENOENT;
                    return 0;
                } else {
                    fprintf(STDERR, "repserver[%d]: Volume does not exist on server as indicated by the VLDB: %lu\n\n", tid, (unsigned long) volid);
                    return 0;
                }
            } else {
                fprintf(STDERR, "examine");
            }
        } else {
            foundserv = 1;

            if ((voltype == BACKVOL) && !(entry.flags & BACK_EXISTS)) {
                /* The VLDB says there is no backup volume yet we found one on disk */
                fprintf(STDERR, "repserver[%d]: VLDB reports no backup volume, however we found one on disk: %lu\n\n", tid, (unsigned long) volid);
                error = ENOENT;
                return 0;
            }
        }
        ro = pntr->updateDate;

        if (rw != ro) ROtally++;
        if (pntr)
            free(pntr);
    } while (voltype == ROVOL);

    if (ROtally > 0) {
        return 1;
    } else {
        return 0;
    }
}

void *BossThread(void *arg) {
    struct VldbListByAttributes attributes;
    nbulkentries arrayEntries;
    struct nvldbentry *vllist;
    afs_int32 centries, nentries = 0, vcode, thisindex, nextindex;
    afs_uint32 volid;
    attributes.Mask = 0;
    int j;
    pid_t tid;
    tid = syscall(SYS_gettid);

    fprintf(STDERR, "repserver[%d]: Boss thread started\n", tid);

    while (keepRunning) {
        sleep(polltime);

        for (thisindex = 0; (thisindex != -1); thisindex = nextindex) {
            memset(&arrayEntries, 0, sizeof (arrayEntries));
            centries = 0;
            nextindex = -1;

            vcode =
                    VLDB_ListAttributesN2(&attributes, 0, thisindex, &centries,
                    &arrayEntries, &nextindex);
            if (vcode == RXGEN_OPCODE) {
                /* Vlserver not running with ListAttributesN2. Fall back */
                vcode =
                        VLDB_ListAttributes(&attributes, &centries, &arrayEntries);
                nextindex = -1;
            }
            if (vcode) {
                fprintf(STDERR, "repserver[%d]: Could not access the VLDB for attributes\n\n", tid);
                PrintError("", vcode);
            }
            nentries += centries;

            /* Build list of volumes for our jobs to process */
            for (j = 0; j < centries; j++) {
                vllist = &arrayEntries.nbulkentries_val[j];

                /* If volume is not LOCKED we check it's status */
                if (!(vllist->flags & VLOP_ALLOPERS)) {
                    volid = vllist->volumeId[RWVOL];
                    int status = SyncStatus(volid);

                    if (status) {
                        int *value = malloc(sizeof (*value));
                        *value = volid;
                        fprintf(STDERR, "repserver[%d]: Volume out of sync. Queuing volume[%d] for release.\n", tid, volid);
                        queue_enqueue(arg, value);
                    }
                }

            }
            xdr_free((xdrproc_t) xdr_nbulkentries, &arrayEntries);
        }
        rx_Finalize();
    }
    
    /* Fill the queue with 1's so the workers will exit */
    for(j=0; j<maxthreads; j++) {
        queue_enqueue(arg, 1);
    }

    rx_Finalize();
    fprintf(STDERR, "repserver[%d]: Boss Thread exiting.\n", tid);
    pthread_exit(0);
}

void *WorkerThread(void *arg) {
    pid_t tid;
    tid = syscall(SYS_gettid);
    int rl = 0, a;

    /* Signal handler init */
    for (a = 0; a < 16; a++) {
        if(signal(types[a].num, signal_handler) == SIG_IGN)
            signal(SIGINT, SIG_IGN);
    }    
    
    fprintf(STDERR, "repserver[%d]: Worker thread started.\n", tid);

    while(1) {
        fprintf(STDERR, "repserver[%d]: keepRunning is [%i]\n", tid, keepRunning);
        usleep(20000);
        int *value = queue_dequeue(arg);

        /* Make sure we got something from the queue before attempting to release*/
        if ((*(int *) value) > 0) {
            fprintf(STDERR, "repserver[%d]: Releasing volume[%d]\n", tid, *(int *) value);
            rl = ReleaseVolume(*(int *) value);

            if (rl == -1) {
                fprintf(STDERR, "repserver[%d]: Volume release failed for: %d\n", tid, *(int *) value);
            }
            rx_Finalize();
        } else if ((*(int *) value) == 1){ /* A "1" has been placed in the queue instructing 
                                            this thread to exit */
            free(value);
            break;
        }
        free(value);
    }
    
    fprintf(STDERR, "repserver[%d]: Worker Thread exiting.\n", tid);
    pthread_exit(0);
}

void usage() {
    fprintf(STDERR, "Usage: repserver [-polltime <how often to poll for volume updates>] [-maxthreads <number of processes>]\n");
    exit(0);
}

int main(int argc, char **argv) {
    afs_int32 code;
    int a = 0, c;
    pthread_t boss_tid;
    pthread_t worker_tid[THREADMAX];
    void *buffer[buffer_size];
    queue_t queue = QUEUE_INITIALIZER(buffer);
    pid_t tid;
    tid = syscall(SYS_gettid);
    
    for (c = 1; c < argc; c++) {

        if (strcmp(argv[c], "-help") == 0) {
            usage();
        }

        if (strcmp(argv[c], "-h") == 0) {
            usage();
        }

        if (strcmp(argv[c], "-polltime") == 0) {
            polltime = atoi(argv[++c]);
            if ((polltime > 65535) || (polltime < 0)) {
                polltime = POLLTIME;
                fprintf(STDERR, "repserver: -polltime: invalid argument. Select a polltime between 1-65535. Setting default: %i\n", polltime);
            }
        }

        if (strcmp(argv[c], "-maxthreads") == 0) {
            maxthreads = atoi(argv[++c]);
            if ((maxthreads > 8) || (maxthreads < 0)) {
                maxthreads = MAXTHREADS;
                fprintf(STDERR, "repserver: -maxthreads: invalid argument. Specify a maxthreads value between 1-8. Setting default: %i\n", maxthreads);
            }
        }
    }

    /* Signal handler init */
    for (a = 0; a < 16; a++) {
        if(signal(types[a].num, signal_handler) == SIG_IGN)
            signal(SIGINT, SIG_IGN);
    }

    /* Initialize dirpaths */
    if (!(initAFSDirPath() & AFSDIR_SERVER_PATHS_OK)) {
        fprintf(STDERR, "repserver: Unable to obtain AFS server directory.\n\n");
        exit(2);
    }

    /* Open VolserLog and map stdout, stderr into it */
    OpenLog("/usr/afs/logs/RepserLog");

    confdir = AFSDIR_CLIENT_ETC_DIRPATH;

    fprintf(STDERR, "repserver: Server startup: Max threads: %i, Poll interval: %i\n", maxthreads, polltime);

    /* Init ubik client */
    if ((code = ubikConnect())) {
        fprintf(STDERR, "repserver: Couldn't create ubik client!\n.");
        exit(2);
    }

    /* Spawn boss */
    pthread_create(&boss_tid, NULL, BossThread, &queue);

    /* Spawn workers */
    for (a = 0; a < maxthreads; a++) {
        pthread_create(&worker_tid[a], NULL, WorkerThread, &queue);
    }

    /* Wait for boss thread*/
    pthread_join(boss_tid, NULL);

    /* Wait for worker threads */
    for (a = 0; a < maxthreads; a++) {
        pthread_join(worker_tid[a], NULL);
    }

    fprintf(STDERR, "repserver[%d]: Server shutdown complete.\n", tid);
    
    exit(0);
}
