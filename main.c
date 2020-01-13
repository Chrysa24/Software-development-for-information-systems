#include <stdio.h>
//#include "work.h"
#include "header.h"

pthread_cond_t batch_cond = PTHREAD_COND_INITIALIZER;
  
pthread_mutex_t batch_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t joblist_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t sollist_mut = PTHREAD_MUTEX_INITIALIZER;

sem_t job_list;
sem_t job;
int flag_t = 0; // Flag to terminate the threads
int batch_counter = 0;

Job* JobHead; // Head of the Job List
Solution* Sol_List = NULL;

void InsertSolList(Solution** head, Solution *node){
	if(*head == NULL){
		*head = node;
	}
	else if((*head)->key > node->key){
		node->next = (*head);
		*head = node;
	}
	else{
		Solution *temp = *head;
		while(temp->next != NULL && temp->next->key < node->key)
			temp = temp->next;
		node->next = temp->next;
		temp->next = node;
	}
}

void QueryThread(Qjob* pointer){

	Solution* result;
	result = FindSolution(pointer->commandListHead, pointer->arrayname, pointer->Table, NULL, NULL);
	result->key = pointer->count;

	// Add on the solution list
	pthread_mutex_lock(&sollist_mut);
		InsertSolList(&Sol_List, result);
	pthread_mutex_unlock(&sollist_mut);

	pthread_mutex_lock(&batch_mut);
		batch_counter -- ;
		pthread_cond_signal(&batch_cond);
	pthread_mutex_unlock(&batch_mut);

	DeleteArraylist(&(pointer->commandListHead->arraylist));
	commandListFree(pointer->commandListHead);
	free(pointer);
}

Qjob* PopJob(Job* JobHead){
	Qjob* temp = JobHead->qjobs;
	JobHead->counter --;
	JobHead->qjobs = JobHead->qjobs->next;
	return temp;
}

void *ThreadFunc(void *vargp){

	while(1){
		sem_wait(&job);
		
		if(flag_t == 1)
			return;

		pthread_mutex_lock(&joblist_mut);
			Qjob* current_job = PopJob(JobHead);
		pthread_mutex_unlock(&joblist_mut);

		if(current_job == NULL){
			printf("Somebody woke me up for no reason.\n");
			continue;
		}

		QueryThread(current_job);
	}
}

void InitializeJobsHead(Job **head){
	*head = (Job*)malloc(sizeof(Job));
	(*head)->qjobs = NULL;
	(*head)->counter = 0;
}

void AddJob(Job *head, Qjob *node){
	if(head->qjobs == NULL)
		head->qjobs = node;
	else{
		Qjob* temp;
		temp = head->qjobs;
		head->qjobs = node;
		node->next = temp;
	}
	head->counter ++;
}

int main(int argc, char* argv[]){

	if(argc!=2){
		printf("Correct syntax: ./prog -flag.\nFlag can be -s or -m based on the input file.\n");
		return -1;
	}

	int Flag = 0, num_of_files, num_of_queries, batchend;
	if(strcmp(argv[1], "-s") == 0)
		Flag=1;
	else if(strcmp(argv[1], "-m") == 0)
		Flag=2;
	else if(strcmp(argv[1], "c") == 0)
		Flag = 3;

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

	if(Flag == 1)	
		ReadInputFiles("/tmp/workloads/small/small.init",arrayname,Table,Flag);
	else if(Flag == 2)
		ReadInputFiles("/tmp/workloads/medium/medium.init",arrayname,Table,Flag);
	else if(Flag == 3)
		ReadInputFiles("small.init",arrayname,Table,Flag);

	// Semaphores
	sem_init(&job,1,0);
	sem_init(&job_list, 1, 1); // Only one can have access to the job list
	
	// Create threads
	pthread_t thread_id;
	for(int i=0 ; i<1 ; i++){
		pthread_create(&thread_id, NULL, ThreadFunc, NULL);
	}

	InitializeJobsHead(&JobHead);

	for(int i = 0; i<num_of_queries ;i++){

		COMMANDLIST *commandListHead = NULL;
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
		argument->commandListHead = commandListHead;

		// Add job in the list
		pthread_mutex_lock(&joblist_mut);
			AddJob(JobHead, argument);
		pthread_mutex_unlock(&joblist_mut);

		// Wake up a thread
		sem_post(&job);

		// A new query has been on the works
		batch_counter++;
	}		
	flag_t = 1;

	for(int i=0 ; i<10 ; i++){
		sem_post(&job);
	}

	// Free everything
	
	//commandListFree(commandListHead);

	uint64_t i,j;
	for(i=0; i<num_of_files; i++){
		for(j=0 ; j< Table[i].columns ; j++)
			free(arrayname[i][j]);
		free(arrayname[i]);
	}
	free(arrayname);
	free(Table);

	// Destroy Semaphores
	sem_destroy(&job);
	sem_destroy(&job_list);
	pthread_cond_destroy(&batch_cond);
	
	return 0;
}
