/* Shared scalar expression language used by vector sets and stream reads.
 * Copyright (c) 2009-Present, Redis Ltd.
 * Licensed under RSALv2, SSPLv1, or AGPLv3. See LICENSE.txt. */
#ifndef REDIS_EXPR_H
#define REDIS_EXPR_H
#include <stddef.h>

#define EXPR_TOKEN_EOF 0
#define EXPR_TOKEN_NUM 1
#define EXPR_TOKEN_STR 2
#define EXPR_TOKEN_TUPLE 3
#define EXPR_TOKEN_SELECTOR 4
#define EXPR_TOKEN_OP 5
#define EXPR_TOKEN_NULL 6

#define EXPR_OP_OPAREN 0  /* ( */
#define EXPR_OP_CPAREN 1  /* ) */
#define EXPR_OP_NOT    2  /* ! */
#define EXPR_OP_POW    3  /* ** */
#define EXPR_OP_MULT   4  /* * */
#define EXPR_OP_DIV    5  /* / */
#define EXPR_OP_MOD    6  /* % */
#define EXPR_OP_SUM    7  /* + */
#define EXPR_OP_DIFF   8  /* - */
#define EXPR_OP_GT     9  /* > */
#define EXPR_OP_GTE    10 /* >= */
#define EXPR_OP_LT     11 /* < */
#define EXPR_OP_LTE    12 /* <= */
#define EXPR_OP_EQ     13 /* == */
#define EXPR_OP_NEQ    14 /* != */
#define EXPR_OP_IN     15 /* in */
#define EXPR_OP_AND    16 /* and */
#define EXPR_OP_OR     17 /* or */

/* This structure represents a token in our expression. It's either
 * literals like 4, "foo", or operators like "+", "-", "and", or
 * field selectors, that start with a dot, such as ".age". Resolution is
 * supplied by the caller; the language does not implement nested paths. */
typedef struct exprtoken {
    int refcount;           // Reference counting for memory reclaiming.
    int token_type;         // Token type of the just parsed token.
    int offset;             // Chars offset in expression.
    union {
        double num;         // Value for EXPR_TOKEN_NUM.
        struct {
            char *start;    // String pointer for EXPR_TOKEN_STR / SELECTOR.
            size_t len;     // String len for EXPR_TOKEN_STR / SELECTOR.
            char *heapstr;  // True if we have a private allocation for this
                            // string. When possible, it just references to the
                            // string expression we compiled, exprstate->expr.
        } str;
        int opcode;         // Opcode ID for EXPR_TOKEN_OP.
        struct {
            struct exprtoken **ele;
            size_t len;
        } tuple;            // Tuples are like [1, 2, 3] for "in" operator.
    };
} exprtoken;

typedef struct exprstate exprstate;
/* The resolver returns an owned token, or NULL to reject the entry.
 * Borrowed string bytes must remain valid for the duration of exprRun(). */
typedef exprtoken *(*exprResolver)(void *privdata, const char *field, size_t len);
exprstate *exprCompile(const char *expr, int *errpos);
int exprRun(exprstate *es, exprResolver resolver, void *privdata);
void exprFree(exprstate *es);
exprtoken *exprNewToken(int type);
void exprTokenRelease(exprtoken *t);
#endif
