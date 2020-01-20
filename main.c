#include "header.h"


//Global variables

pthread_cond_t batch_cond = PTHREAD_COND_INITIALIZER;
  
pthread_mutex_t batch_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t joblist_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t sjoblist_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t sollist_mut = PTHREAD_MUTEX_INITIALIZER;

sem_t qthread, sthread;
int flag_t = 0; // Flag to terminate the threads
int batch_counter = 0;

Job* JobHead; // Head of the Jobs List
Solution* Sol_List = NULL;

int main(int argc, char* argv[]){

	if(argc < 2){
		printf("Correct syntax: ./prog -flag.\nFlag can be -s or -m based on the input file.\n");
		return -1;
	}

	int Flag = 0, num_of_files, num_of_queries, batchend;
	int qthreads = 3, sthreads = 3;

	if(strcmp(argv[1], "-s") == 0)
		Flag=1;
	else if(strcmp(argv[1], "-m") == 0)
		Flag=2;
	else if(strcmp(argv[1], "c") == 0)
		Flag = 3;

	if(argc > 2){
		qthreads = atoi(argv[2]);
		sthreads = atoi(argv[3]);
	}

	if(Flag == 0){
		printf("Correct syntax: ./prog -flag.\n Flag can be -s or -m based on the input file.\n");
		return -1;
	}

	if(Flag==1){
		num_of_queries= Countrows("/tmp/workloads/small/small.work");
		num_of_files= Countrows("/tmp/workloads/small/small.init");
	}
	else if(Flag==2){
		num_of_queries =Countrows("/tmp/workloads/medium/medium.work");
		num_of_files= Countrows("/tmp/workloads/medium/medium.init");
	}
	else if(Flag == 3){
		num_of_queries= Countrows("small.work");
		num_of_files= Countrows("small.init");
	}

	Initial_Table* Table;
	Table = malloc(sizeof(Initial_Table)* num_of_files);

	// An array with all the arrays of records
	uint64_t ***arrayname;
	arrayname = malloc(sizeof(uint64_t**)*num_of_files);

	// An array with all statistics of files
	Statistics* initStats[num_of_files];
	
	
	if(Flag == 1)	
		ReadInputFiles("/tmp/workloads/small/small.init",arrayname,Table,Flag,initStats);
	else if(Flag == 2)
		ReadInputFiles("/tmp/workloads/medium/medium.init",arrayname,Table,Flag,initStats);
	else if(Flag == 3)
		ReadInputFiles("small.init",arrayname,Table,Flag,initStats);

	// Semaphores
	sem_init(&qthread,1,0);
	sem_init(&sthread,1,0);
	
	// Create threads
	pthread_t thread_id;
	for(int i=0 ; i<qthreads ; i++){
		pthread_create(&thread_id, NULL, QueryThread, (void**)initStats);
	}
	for(int i=0 ; i<sthreads ; i++){
		pthread_create(&thread_id, NULL, SortThread, NULL);
	}
	
	COMMANDLIST *commandListHead = NULL;


	InitializeJobsHead(&JobHead);

	for(int i = 0; i<num_of_queries ;i++){

		commandListHead = commandListInit(commandListHead);
		commandListHead->lineCounter = i;
		
		batchend = readCommand(commandListHead,Flag);

		if(batchend == 0){
			// This is the end of a batch

			// Wait for the queries to end
			pthread_mutex_lock(&batch_mut); 
				while(batch_counter != 0){
					pthread_cond_wait(&batch_cond, &batch_mut);
				}
			pthread_mutex_unlock(&batch_mut);
			
			// Print Solutions
			pthread_mutex_lock(&sollist_mut);
				PrintSolution(Sol_List);
				DeleteSolution(&Sol_List);
			pthread_mutex_unlock(&sollist_mut);

			printf("F\n");
			
			continue;
		}
		// Create a query job
		Qjob *argument;
		argument = (Qjob*)malloc(sizeof(Qjob));

		argument->count = i;
		argument->Flag = Flag;
		argument->arrayname = arrayname;
		argument->Table = Table;
		argument->next = NULL;
		argument->commandListHead = commandListHead;

		// Add job in the list
		pthread_mutex_lock(&joblist_mut);
			AddJob(JobHead, argument);
		pthread_mutex_unlock(&joblist_mut);

		// Wake up a thread
		sem_post(&qthread);

		// A new query has been on the works
		batch_counter++;
	}
	flag_t = 1;

	for(int i=0 ; i<qthreads ; i++){
		sem_post(&qthread);
	}
	for(int i=0 ; i<sthreads ; i++){
		sem_post(&sthread);
	}

	// Free everything
	
//	commandListFree(commandListHead);


	// Delete Statistics
	DeleteinitStats(initStats,num_of_files,Table);
	
	uint64_t i,j;
	for(i=0; i<num_of_files; i++){
		for(j=0 ; j< Table[i].columns ; j++)
			free(arrayname[i][j]);
		free(arrayname[i]);
	}
	free(arrayname);
	free(Table);

	free(JobHead);
	// Destroy Semaphores
	sem_destroy(&qthread);
	sem_destroy(&sthread);
	pthread_cond_destroy(&batch_cond);

	printf("\nQuery threads: %d\nSort threads: %d\n", qthreads, sthreads);
	return 0;
}
