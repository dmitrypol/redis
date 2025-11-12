#include "../../redismodule.h"
#include <pthread.h>

void *my_thread_function() {
    RedisModuleCtx *ts_ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_ThreadSafeContextLock(ts_ctx);
    RedisModule_Log(ts_ctx, "notice", "my_thread_function");
    RedisModule_ThreadSafeContextUnlock(ts_ctx);
    RedisModule_FreeThreadSafeContext(ts_ctx);

    return NULL;
}

int cmd1(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    pthread_t thread_id;
    pthread_create(&thread_id, NULL, my_thread_function, NULL);
    pthread_detach(thread_id);

    RedisModule_Log(ctx,"notice","cmd1");;
    return RedisModule_ReplyWithSimpleString(ctx, "cmd1");
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "bgthread", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "cmd1", cmd1, "", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

     return REDISMODULE_OK;
}