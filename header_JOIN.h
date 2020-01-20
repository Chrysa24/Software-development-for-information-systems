#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h> 

typedef struct Bucket {
    int flag;
	uint64_t start, end; // Starting and ending point in the table
	int numofbyte; // The byte and the number of the byte that is hashed upon
    struct Bucket * next;
} bucket;


typedef struct record{
	uint64_t rowIdA;
	uint64_t rowIdB;
	struct record* next;
}Record;


typedef struct result{
	Record* buffer;
	uint64_t num_of_records;
	uint64_t max_records;
	struct result* next;
}Result;


typedef struct array{
	uint64_t rows;
	uint64_t columns;
	uint64_t** pointers;
}Initial_Table;


typedef struct list{
	int64_t* tuple;
	struct list* next;
}RowIds;


typedef struct FilteredRowIds{
	int64_t id;
	struct FilteredRowIds* next;
}FilRowIds;


typedef struct Res{
	int64_t rows;
	int64_t column;
	FilRowIds* filtered;
}Current_Table;


typedef struct solutions{
	uint64_t* sum;
	int counter;
	int key;
	struct solutions* next;
}Solution;


typedef struct node{
    __uint64_t firstArray;
    __uint64_t firstCol;
    __uint64_t secondArray;
    __uint64_t secondCol;

    /*command type is a flag that lets us know what command we should execute */
    /*for example 1="&", 2="<" , 3=">", 4="=" etc. */
    int commandType;
    struct node *next;
}NODE;


typedef struct arrays{
	int arrayname;
	struct arrays* next;
}Arrays;


typedef struct projection{
    __uint64_t array;
    __uint64_t col;

    struct projection *next;
}PROJECTION;


typedef struct commandList {
	NODE *nodes;
	PROJECTION *projections;
	Arrays* arraylist;
	int num_of_files;
	int lineCounter;
	int projectionCount;
}COMMANDLIST;

typedef struct QueryJob{
	uint64_t ***arrayname;
	Initial_Table* Table;
	int Flag;
	int count; // Index of the query
	COMMANDLIST *commandListHead;
	struct QueryJob *next;
} Qjob;

typedef struct Merging_Parts{
	uint64_t rows;
	int key;
	RowIds * head;
	struct Merging_Parts *next;
} Merge;

typedef struct SortJob{
	bucket *mybucket;
	uint64_t** From, ** To; // The tables for sorting
	int columns;
	int flag; //Specify the job
	pthread_cond_t* thread_cond;
	pthread_mutex_t* thread_mut;
	int* waiting;
	struct SortJob *next;

	// Jjob
	uint64_t start, end, rowsB;
	int arraynumber1, arraynumber2, key;
	Merge** head;
} Sjob;

typedef struct JOB_HEAD_NODE{
	Qjob *qjobs;
	Sjob* sjobs;
} Job;

typedef struct statistics{
	int64_t Ia;
	int64_t Ua;
	int64_t Fa;
	int64_t Da;
	bool* check;
}Statistics;




// JOBS_THREADS 

void InsertSolList(Solution** , Solution*);
void WorkingOnQuery(Qjob* , pthread_cond_t* , pthread_mutex_t* , int* ,Statistics**, Merge**);
Qjob* PopJob(Job*);
Sjob* SPopJob(Job*);
int BucketsToJobs(bucket* , Job *, uint64_t** , uint64_t** , pthread_cond_t* , pthread_mutex_t* , int* , int);
int BucketsToJobsTriple(bucket* , Job*, uint64_t** , uint64_t** , int , pthread_cond_t* , pthread_mutex_t* , int* , int);
void *QueryThread(void**);
void *SortThread(void *);
void InitializeJobsHead(Job **);
void AddJob(Job* , Qjob*);
void SAddJob(Job *, Sjob *);
void DeleteMergeList(Merge **);

// STATISTICS

void InformStatistics(Statistics** ,int , int ,uint64_t , int , int ,uint64_t , int);
int newSort(COMMANDLIST *,int* , Initial_Table* , Statistics**, Statistics**);
int Evaluate(COMMANDLIST* , Statistics**,Statistics** ,int ,int,int* ,Initial_Table*);
int64_t checkStats(Statistics** ,int , int , int , int);
void DeleteStats(Statistics** ,int);
void DeleteinitStats(Statistics** ,int,Initial_Table*);
void CopyStats(Statistics** ,Statistics** ,int* ,Initial_Table *,int);
float power(float , int);

// FILTER_JOIN

int Filter(uint64_t** , uint64_t ,uint64_t , int, int,Current_Table*,int, pthread_cond_t*, pthread_mutex_t*, int*);
int SelfJoin(RowIds**, uint64_t** ,uint64_t, uint64_t , uint64_t, Current_Table*,int,int, pthread_cond_t*, pthread_mutex_t*, int*);
int Join(RowIds**, uint64_t**,uint64_t** ,uint64_t , uint64_t ,uint64_t, uint64_t ,Current_Table**, Current_Table* ,Current_Table* ,int ,int ,int, pthread_cond_t*, pthread_mutex_t*, int*, Merge**);
int64_t EasyJoin(RowIds**,uint64_t**,uint64_t**,uint64_t, uint64_t, int,int,int);
uint64_t JoinList(Record** ,uint64_t ** ,uint64_t ** ,uint64_t ,uint64_t, int ,int ,int , RowIds** ,pthread_cond_t*, pthread_mutex_t*, int*, Merge**);
uint64_t JoinListThread(RowIds**, uint64_t ** ,uint64_t ** , uint64_t, uint64_t, uint64_t , int,int,int);
int Synchronize(RowIds** , uint64_t ***, uint64_t , Current_Table* , uint64_t** , int, int, uint64_t, int);
uint64_t FindResult(RowIds* , uint64_t**, uint64_t, int);
uint64_t IndexFromTuples(RowIds** , Current_Table* , int64_t ***, int, uint64_t, int, uint64_t**);
void TuplesFromIndex(RowIds** , Current_Table* , int64_t ***, int , uint64_t );
void SyncRows(Current_Table** , int64_t , int);
void DestroyCols(Current_Table**, int );
Solution* FindSolution(COMMANDLIST *,uint64_t***,Initial_Table*,Solution**,Solution*, pthread_cond_t*, pthread_mutex_t*, int*,Statistics**, Merge**);


//DELETE FUNCTIONS
void FreeIndex(uint64_t **,uint64_t);
RowIds* DeleteId(RowIds**,RowIds* ,RowIds*);
FilRowIds* DeleteFilterId(FilRowIds** ,FilRowIds*,FilRowIds*);
void DeleteCurFilList(FilRowIds**);
void DeleteJoinList(Record**);
void DeleteSolution(Solution**);
void DeleteArraylist(Arrays**);


//INPUT FUNCTIONS
void ReadFilesRecords(char* ,uint64_t*** ,Initial_Table* ,int,Statistics**);
int Countrows(char*);
void ReadInputFiles(char* File,uint64_t***,Initial_Table*,int,Statistics**);

// PRINT FUNCTIONS
void PrintTable(uint64_t** , uint64_t);
void PrintBucketNode(bucket* );
void PrintHist(int*);
void PrintList(Record *);
void PrintRows(RowIds*,int);
void PrintFilRows(FilRowIds *,uint64_t **, uint64_t);
void PrintSolution(Solution*);
void PrintArraylist(Arrays*);
void projectionPrint(PROJECTION *projectionHead);
void nodePrint(NODE *nodeHead);

// INSERT FUNCTIONS
uint64_t CreateNewIndex(Current_Table* , uint64_t, uint64_t** , uint64_t, uint64_t ***,int, pthread_cond_t*, pthread_mutex_t*, int*);
void InitializeTuple(int64_t*, int);
RowIds* InsertId(RowIds** , RowIds*, uint64_t, uint64_t ,int, int ,int);
RowIds* InsertId2(RowIds**, RowIds* , uint64_t, int , int , RowIds**);
FilRowIds* InsertFilterId(FilRowIds** , FilRowIds*, uint64_t);
void Initialize_Cur_Table(Current_Table*, int);
void Insert_Result(Record** , uint64_t , uint64_t, Record**);
RowIds* InsertIdSyn(RowIds**, Current_Table *, RowIds*, int);
Arrays* InsertArraylist(Arrays** ,Arrays* , int);
Solution* InsertSolution(Solution**, Solution*, int);

// SORT FILE
void CreateBucketList(bucket **, int* , uint64_t*);
int DivideBucket(bucket **, int* , uint64_t* , bucket *, int);
void RemoveBucket(bucket **, bucket *);
void ReadFile(char* , uint64_t**);
uint64_t CountRows(char*);
void CreateIndexTable(uint64_t** , uint64_t** ,uint64_t,uint64_t);
void SetZero(int* , int);
int GetByte(uint64_t , int);
void CreateHist(uint64_t** , int* , uint64_t , uint64_t , int);
void CreatePsum(int *, uint64_t *);
void Reorder(uint64_t** , uint64_t** , uint64_t* , uint64_t , uint64_t , int);
void CopyBucket(uint64_t** , uint64_t** , uint64_t , uint64_t );
uint64_t Partition(uint64_t** , int64_t , int64_t);
void Myqsort(uint64_t** , int64_t , int64_t);
void FinalizeTables(uint64_t** , uint64_t** , bucket **, pthread_cond_t*, pthread_mutex_t*, int*);
void FinalizeTablesThread(uint64_t**, uint64_t**, bucket **);
void CompareTables(uint64_t ** ,uint64_t ** ,uint64_t , char*);
void Sort(uint64_t** , uint64_t, pthread_cond_t* , pthread_mutex_t*, int*);


void CreateHistTriple(int64_t** , int* , int64_t , int64_t , int);
void ReorderTriple(int64_t** , int64_t** , uint64_t* , int64_t , int64_t , int , int);
uint64_t PartitionTriple(int64_t** , int64_t , int64_t, int);
void SortTriple(int64_t** , uint64_t , int,pthread_cond_t* , pthread_mutex_t*, int*);
void FinalizeTablesTriple(int64_t** , int64_t** , bucket **, int, pthread_cond_t*, pthread_mutex_t*, int*);
void FinalizeTablesThreadTriple(uint64_t** , uint64_t** , bucket **, int);
void CopyBucketTriple(int64_t** , int64_t** , uint64_t , uint64_t, int);
void MyqsortTriple(int64_t**, int64_t , int64_t , int);


// WORK
COMMANDLIST *commandListInit(COMMANDLIST *commandListHead);
void commandListFree(COMMANDLIST *commandListHead);
int readCommand(COMMANDLIST *commandListHead,int);
void swapWorkNodes(NODE *node1, NODE *node2);
void commandListSort(COMMANDLIST *commandListHead);
void nodeInsert(NODE *nodeHead, __uint64_t firstArray, __uint64_t firstCol, __uint64_t secondArray, __uint64_t secondCol, int commandType);
NODE *nodeInit(NODE *nodeHead);
void nodeFree(NODE *nodeHead);
PROJECTION *projectionInit(PROJECTION *projectionHead);
void projectionFree(PROJECTION *projectionHead);
void projectionInsert(PROJECTION *projectionHead, __uint64_t array, __uint64_t col);
