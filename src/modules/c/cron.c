#include "../../redismodule.h"
#include <time.h>
#include <string.h>
#include <stdbool.h>

static const double FREQUENCY_IN_SECONDS = 60.0;
static const uint64_t AVAILABLE_MEMORY = 1024 * 1024 * 1024; // 1 GB
static const uint64_t MASTER_REPLICA_OFFSET_DIFF = 10 * 1024 * 1024; // 10 MB
RedisModuleCallReply *rmc_reply;

void cron_loop_callback(RedisModuleCtx *ctx, RedisModuleEvent event, uint64_t subevent, void *data);
int client_output_buffer_limit_replica(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
bool is_primary(RedisModuleCtx *ctx);
uint64_t get_master_repl_offset(RedisModuleCtx *ctx);
uint64_t get_available_memory(RedisModuleCtx *ctx);
uint64_t get_max_replica_offset(RedisModuleCtx *ctx);

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    if (RedisModule_Init(ctx, "mycron", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_CronLoop, cron_loop_callback) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"coblr", client_output_buffer_limit_replica,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}

void cron_loop_callback(RedisModuleCtx *ctx, RedisModuleEvent event, uint64_t subevent, void *data) {
    RedisModule_AutoMemory(ctx);
    REDISMODULE_NOT_USED(subevent);
    REDISMODULE_NOT_USED(data);
    if (event.id == REDISMODULE_EVENT_CRON_LOOP) {
        static time_t last_run = 0;
        time_t now = time(NULL);
        if (difftime(now, last_run) >= FREQUENCY_IN_SECONDS) {
            last_run = now;
            client_output_buffer_limit_replica(ctx, NULL, 0);
        }
    }
}

int client_output_buffer_limit_replica(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    if (!is_primary(ctx)) return REDISMODULE_ERR;;
    uint64_t master_repl_offset = get_master_repl_offset(ctx);
    uint64_t max_replica_offset = get_max_replica_offset(ctx);
    uint64_t available_memory = get_available_memory(ctx);
    RedisModule_Log(ctx, "notice", "master_repl_offset: %llu, max_replica_offset: %llu, available_memory: %llu",
        master_repl_offset, max_replica_offset, available_memory);
    uint64_t master_replica_offset_diff = master_repl_offset - max_replica_offset;
    if (available_memory > AVAILABLE_MEMORY && master_replica_offset_diff > MASTER_REPLICA_OFFSET_DIFF) {
        rmc_reply = RedisModule_Call(ctx, "CONFIG", "ccc!", "SET", "client-output-buffer-limit", "replica 512mb 64mb 60");
        RedisModule_Log(ctx, "notice", "config set client-output-buffer-limit replica 512mb 64mb 60");
    }
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
}

bool is_primary(RedisModuleCtx *ctx) {
    // role:master
    rmc_reply = RedisModule_Call(ctx, "INFO", "c", "replication");
    if (rmc_reply && RedisModule_CallReplyType(rmc_reply) == REDISMODULE_REPLY_STRING) {
        const char *reply_str = RedisModule_CallReplyStringPtr(rmc_reply, NULL);
        return strstr(reply_str, "role:master") != NULL;
    }
    return false;
}

uint64_t get_master_repl_offset(RedisModuleCtx *ctx) {
    // master_repl_offset:9001474
    uint64_t output = 0;
    rmc_reply = RedisModule_Call(ctx, "INFO", "c", "replication");
    if (rmc_reply && RedisModule_CallReplyType(rmc_reply) == REDISMODULE_REPLY_STRING) {
        const char *reply_str = RedisModule_CallReplyStringPtr(rmc_reply, NULL);
        const char *mro = strstr(reply_str, "master_repl_offset:");
        if (mro) {
            output = atoll(mro + strlen("master_repl_offset:"));
        }
        RedisModule_Log(ctx, "notice", "master_repl_offset: %llu", output);
    }
    return output;
}

uint64_t get_available_memory(RedisModuleCtx *ctx) {
    // maxmemory - used_memory_rss
    // used_memory_rss:3440640
    // maxmemory:2147483648
    uint64_t output = 0;
    rmc_reply = RedisModule_Call(ctx, "INFO", "c", "memory");
    if (rmc_reply && RedisModule_CallReplyType(rmc_reply) == REDISMODULE_REPLY_STRING) {
        const char *reply_str = RedisModule_CallReplyStringPtr(rmc_reply, NULL);
        const char *mm = strstr(reply_str, "maxmemory:");
        const char *umr = strstr(reply_str, "used_memory_rss:");
        if (mm && umr) {
            uint64_t maxmemory = atoll(mm + strlen("maxmemory:"));
            uint64_t used_memory_rss = atoll(umr + strlen("used_memory_rss:"));
            output = maxmemory - used_memory_rss;
            RedisModule_Log(ctx, "notice", "maxmemory: %llu, used_memory_rss: %llu, available_memory: %llu",
                maxmemory, used_memory_rss, output);
        }
    }
    return output;
}

uint64_t get_max_replica_offset(RedisModuleCtx *ctx) {
    // loop through replica records to get max offset value
    // connected_slaves:2
    // slave0:ip=127.0.0.1,port=7379,state=online,offset=39805946,lag=26
    // slave1:ip=127.0.0.1,port=8379,state=online,offset=80580088,lag=0
    uint64_t output = 0;
    rmc_reply = RedisModule_Call(ctx, "INFO", "c", "replication");
    if (rmc_reply && RedisModule_CallReplyType(rmc_reply) == REDISMODULE_REPLY_STRING) {
        const char *reply_str = RedisModule_CallReplyStringPtr(rmc_reply, NULL);
        const char *cs = strstr(reply_str, "connected_slaves:");
        if (cs) {
            int num_replicas = atoi(cs + strlen("connected_slaves:"));
            for (int i = 0; i < num_replicas; i++) {
                char replica_key[32];
                snprintf(replica_key, sizeof(replica_key), "slave%d:", i);
                const char *replica_record = strstr(reply_str, replica_key);
                if (replica_record) {
                    const char *offset_str = strstr(replica_record, "offset=");
                    if (offset_str) {
                        uint64_t offset = atoll(offset_str + strlen("offset="));
                        RedisModule_Log(ctx, "notice", "slave%d offset: %llu", i, offset);
                        if (offset > output) {
                            output = offset;
                        }
                    }
                }
            }
        }
    }
    return output;;
}
