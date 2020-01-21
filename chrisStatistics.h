#include<stdbool.h>
typedef struct colStatistics{
    bool *arrayCheck;
    uint64_t lA;
    uint64_t uA;
    uint64_t fA;
    uint64_t dA;
}COLSTATISTICS;

typedef struct arrayD{
    COLSTATISTICS *colStats;
    //bool **colStats;
    int columns;
    int rows;
}ARRAYD;


typedef struct arrayStatistics{
	ARRAYD *array;
	int arrayCount;
}ARRAYSTATISTICS;

ARRAYSTATISTICS* createStatistics( uint64_t ***arrayname, Initial_Table* Table, int arrayCount);

ARRAYSTATISTICS* arrayStatsInit(ARRAYSTATISTICS *stats, int arrayCount, uint64_t ***arrayname, Initial_Table* Table);

void freeStatistics(ARRAYSTATISTICS *stats, int arrayCount, Initial_Table* Table);

int commandListStatsCost(COMMANDLIST *commandListHead,ARRAYSTATISTICS *stats);

void updateNodeCost(NODE *command, ARRAYSTATISTICS *stats, Arrays* arraylist);

void updateArrayStats(NODE *command, ARRAYSTATISTICS *stats, Arrays* arraylist);

void nodeListSwap(NODE* node1, NODE* node2);

void bubbleNodeSort(NODE *tempNodeList, int count, int offset);