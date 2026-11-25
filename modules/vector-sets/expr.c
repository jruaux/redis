/* Filtering of objects based on simple expressions.
 * This powers the FILTER option of Vector Sets, but it is otherwise
 * general code to be used when we want to tell if a given object (with fields)
 * passes or fails a given test for scalars, strings, ...
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 * Originally authored by: Salvatore Sanfilippo.
 */

#ifdef TEST_MAIN
#define _DEFAULT_SOURCE
#define _USE_MATH_DEFINES
#define RedisModule_Alloc malloc
#define RedisModule_Realloc realloc
#define RedisModule_Free free
#define RedisModule_Strdup strdup
#define RedisModule_Assert assert
#include "../../src/expr.c"
#else
#include "../../src/expr.h"
#endif
#include "fastjson.c"

typedef struct exprJson {
    const char *data;
    size_t len;
} exprJson;

static exprtoken *exprResolveJson(void *privdata, const char *field, size_t len) {
    exprJson *json = privdata;
    return jsonExtractField(json->data, json->len, field, len);
}

static int exprRunJson(exprstate *es, const char *data, size_t len) {
    exprJson json = {data, len};
    return exprRun(es, exprResolveJson, &json);
}

/* ============================ Simple test main ============================ */

#ifdef TEST_MAIN
#include "fastjson_test.c"

void exprPrintToken(exprtoken *t) {
    switch(t->token_type) {
        case EXPR_TOKEN_EOF:
            printf("EOF");
            break;
        case EXPR_TOKEN_NUM:
            printf("NUM:%g", t->num);
            break;
        case EXPR_TOKEN_STR:
            printf("STR:\"%.*s\"", (int)t->str.len, t->str.start);
            break;
        case EXPR_TOKEN_SELECTOR:
            printf("SEL:%.*s", (int)t->str.len, t->str.start);
            break;
        case EXPR_TOKEN_OP:
            printf("OP:");
            for (int i = 0; ExprOptable[i].opname != NULL; i++) {
                if (ExprOptable[i].opcode == t->opcode) {
                    printf("%s", ExprOptable[i].opname);
                    break;
                }
            }
            break;
        default:
            printf("UNKNOWN");
            break;
    }
}

void exprPrintStack(exprstack *stack, const char *name) {
    printf("%s (%d items):", name, stack->numitems);
    for (int j = 0; j < stack->numitems; j++) {
        printf(" ");
        exprPrintToken(stack->items[j]);
    }
    printf("\n");
}

int main(int argc, char **argv) {
    /* Check for JSON parser test mode. */
    if (argc >= 2 && strcmp(argv[1], "--test-json-parser") == 0) {
        run_fastjson_test();
        return 0;
    }

    char *testexpr = "(5+2)*3 and .year > 1980 and 'foo' == 'foo'";
    char *testjson = "{\"year\": 1984, \"name\": \"The Matrix\"}";
    if (argc >= 2) testexpr = argv[1];
    if (argc >= 3) testjson = argv[2];

    printf("Compiling expression: %s\n", testexpr);

    int errpos = 0;
    exprstate *es = exprCompile(testexpr,&errpos);
    if (es == NULL) {
        printf("Compilation failed near \"...%s\"\n", testexpr+errpos);
        return 1;
    }

    exprPrintStack(&es->tokens, "Tokens");
    exprPrintStack(&es->program, "Program");
    printf("Running against object: %s\n", testjson);
    int result = exprRunJson(es,testjson,strlen(testjson));
    printf("Result1: %s\n", result ? "True" : "False");
    result = exprRunJson(es,testjson,strlen(testjson));
    printf("Result2: %s\n", result ? "True" : "False");

    exprFree(es);
    return 0;
}
#endif
